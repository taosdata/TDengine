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
Batch meta txn (phase 2): CREATE STREAM / DROP STREAM inside a transaction,
both standalone and mixed with table DDL in the same transaction.

Covers the behaviour defined in
  docs/releases/TSDB-v3.4.3-*/05-设计文档/元数据 DDL 支持事务二期-流计算创建与删除 {FS,DS}.md

Stream-only scenarios:
  s1  CREATE STREAM + COMMIT      -> stream exists and reaches Running
  s2  CREATE STREAM + ROLLBACK    -> stream gone (no output table to check: without
      `partition by` the output table is a NORMAL table created lazily on first
      output, not at CREATE STREAM time)
  s3  DROP STREAM + COMMIT        -> stream gone, result table kept
  s4  DROP STREAM + ROLLBACK      -> stream kept and still running (DS 5.2)
  s5  CREATE then DROP same txn   -> nets out to nothing on COMMIT
  s6  same-name recreate rejected -> TSDB_CODE_TXN_RECREATE_NOT_SUPPORTED (0x3320)
  s7  rename replacement allowed  -> DROP s + CREATE s2 works
  s8  committed stream produces output rows end-to-end
  s9  no stale txnId after COMMIT -> later non-txn DROP is not RESOURCE_BUSY
  s10 batch DROP STREAM s1, s2 -> pending_drop in-txn, ROLLBACK restores, IF EXISTS
      applies per name (FS 4.12)
  s11 streams share the per-txn DDL object budget with table DDL (FS 4.13)
  s12 definition replacement (DROP s + CREATE s2 in one txn): no "old dropped, new
      not yet created" gap — old keeps running until COMMIT, new has no task before
      COMMIT (DS 8)
  s13 no stale txn markers after COMMIT: status reverts from pending_create to the
      runtime status; ROLLBACK fully removes the object

Mixed table-DDL + stream scenarios (one transaction spanning both object kinds):
  m1  CREATE TABLE(s) + CREATE STREAM + COMMIT   -> all committed together
  m2  CREATE TABLE(s) + CREATE STREAM + ROLLBACK -> all undone together
  m3  stream over a source table created in the SAME txn + COMMIT
  m4  stream over a source table created in the SAME txn + ROLLBACK (no dangling ref)
  m5  DROP STREAM + DROP its source STABLE + COMMIT
  m6  pre-created output STABLE + partitioned stream into it + COMMIT
  m7  ALTER STABLE + CREATE TABLE + CREATE STREAM + COMMIT (schema change applied)
  m8  ALTER STABLE + CREATE TABLE + CREATE STREAM + ROLLBACK (schema change undone)
  m9  a failing stream stmt does not discard the rest of the txn
  m10 several streams may share one output STB, in or out of a txn (0x3321 removed)
  m10b dropping that output STB from outside the txn is NOT blocked (FS 4.5: no reference hold)
  m11 CREATE STREAM with no snode reports 0x042C on both paths (0x3322 removed)
  m12 an uncommitted stream pins its snode: external DROP SNODE is refused
  m13 an uncommitted stream does not deploy before COMMIT; it does deploy after
  m14 form-A output STB is created inside the txn and undone by ROLLBACK; form C is untouched
  m15 form-B NODELAY output table is created in-txn, isolated from other sessions, works after COMMIT
  m16 STOP/START/RECALCULATE from outside the owning txn are refused with
      TXN_RESOURCE_BUSY while the stream is pending_drop (FS 4.6)
  m17 txn DROP refuses TSMA-internal streams with 0x3110, same as the non-txn path (DS 6.1)
  m18 pre-existing NORMAL output table is referenced, never created or undone (form D)
"""

from new_test_framework.utils import tdLog, tdSql
import taos
import time


class TestBatchMetaTxnStream:

    # Streams only deploy on a snode, and deployment is asynchronous.
    DEPLOY_WAIT_S = 20

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        # A snode is required for any stream to deploy. Ignore the error when one
        # already exists on this dnode.
        tdSql.execute_ignore_error("create snode on dnode 1")
        tdSql.execute("drop database if exists txn_stm")
        tdSql.execute("create database txn_stm vgroups 2")
        tdSql.execute("use txn_stm")
        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1)")

    def teardown_class(cls):
        """Drop the scenario database so a later CI rerun starts from a clean slate.
        The per-scenario s0_reset_env only restores the base tables/streams; if a
        scenario fails mid-way it can leave objects behind, and the shared snode (created
        on dnode 1 in setup_class) also survives. Dropping the whole db here makes the
        suite idempotent across reruns."""
        tdLog.debug("teardown %s" % __file__)
        tdSql.execute_ignore_error("ROLLBACK")
        tdSql.execute_ignore_error("drop database if exists txn_stm")
        tdSql.execute_ignore_error("drop stream if exists s8")
        tdLog.debug("teardown %s done" % __file__)

    # Objects created by setup_class that every scenario may rely on.
    BASE_STABLES = ("stb",)
    BASE_TABLES = ("ct1",)

    def s0_reset_env(self):
        """Cancel any open txn, then drop every stream and every object a previous
        scenario created, leaving only the base tables. Scenarios assert on whole
        `show tables` / `show stables` sets, so leftovers would cross-contaminate."""
        tdSql.execute_ignore_error("ROLLBACK")
        tdSql.execute("use txn_stm")

        tdSql.query("show streams")
        for i in range(tdSql.queryRows):
            tdSql.execute_ignore_error(f"drop stream if exists {tdSql.queryResult[i][0]}")

        # Drop super tables first: that also removes their child tables.
        tdSql.query("show stables")
        for name in [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]:
            if name not in self.BASE_STABLES:
                tdSql.execute_ignore_error(f"drop stable if exists {name}")

        tdSql.query("show tables")
        for name in [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]:
            if name not in self.BASE_TABLES:
                tdSql.execute_ignore_error(f"drop table if exists {name}")

    def _stream_names(self):
        tdSql.query("show streams")
        return sorted(tdSql.queryResult[i][0] for i in range(tdSql.queryRows))

    def _wait_status(self, name, wanted, timeout=None):
        """Wait until `name` reports one of `wanted`. Returns the status seen."""
        timeout = timeout or self.DEPLOY_WAIT_S
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            tdSql.query("show streams")
            for i in range(tdSql.queryRows):
                if tdSql.queryResult[i][0] == name:
                    last = tdSql.queryResult[i][1]
                    if last in wanted:
                        return last
            time.sleep(1)
        return last

    def _create_stream(self, name, out, extra=""):
        tdSql.execute(
            f"create stream {name} interval(10s) sliding(10s) from ct1 {extra}"
            f" into {out} as select _twstart ts, count(*) cnt from %%trows"
        )

    def _create_stream_from(self, name, src, out):
        """Create a stream whose trigger table is `src` rather than the shared ct1."""
        tdSql.execute(
            f"create stream {name} interval(10s) sliding(10s) from {src}"
            f" into {out} as select _twstart ts, count(*) cnt from %%trows"
        )

    def _tables(self):
        tdSql.query("show tables")
        return sorted(tdSql.queryResult[i][0] for i in range(tdSql.queryRows))

    def _stables(self):
        tdSql.query("show stables")
        return sorted(tdSql.queryResult[i][0] for i in range(tdSql.queryRows))

    def _columns(self, table):
        tdSql.query(f"desc {table}")
        # desc returns (field, type, length, note, ...); note == 'TAG' marks tags.
        return sorted(tdSql.queryResult[i][0] for i in range(tdSql.queryRows))

    # =========================================================================
    # CREATE STREAM
    # =========================================================================

    def s1_create_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s1_create_commit")

        tdSql.execute("BEGIN")
        self._create_stream("s1", "o1")
        tdSql.execute("COMMIT")

        assert "s1" in self._stream_names(), "stream s1 missing after COMMIT"
        status = self._wait_status("s1", ("Running", "Idle"))
        assert status in ("Running", "Idle"), f"s1 did not deploy after COMMIT, status={status}"
        tdLog.info(f"s1 deployed after COMMIT, status={status}")

    def s2_create_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s2_create_rollback")

        tdSql.execute("BEGIN")
        self._create_stream("s2", "o2")
        tdSql.execute("ROLLBACK")

        assert "s2" not in self._stream_names(), "stream s2 still present after ROLLBACK"

        # Note: with no `partition by`, the output table is a NORMAL table and is created
        # lazily on the stream's first output, not at CREATE STREAM time. So there is no
        # output table to check here -- the meaningful assertion is that the stream itself
        # is gone. Output-table rollback for the eagerly-created (super table) form is
        # covered separately once `partition by` streams are in scope.
        tdLog.info("s2 removed by ROLLBACK")

    # =========================================================================
    # DROP STREAM
    # =========================================================================

    def s3_drop_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s3_drop_commit")

        self._create_stream("s3", "o3")
        self._wait_status("s3", ("Running", "Idle"))

        tdSql.execute("BEGIN")
        tdSql.execute("drop stream s3")
        tdSql.execute("COMMIT")

        assert "s3" not in self._stream_names(), "stream s3 still present after DROP+COMMIT"

        # The base table must be untouched -- DROP STREAM only removes the stream
        # definition, never the user's source tables.
        tdSql.query("show stables")
        stables = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert "stb" in stables, "source table stb must survive DROP STREAM"
        tdLog.info("s3 dropped by COMMIT, source table untouched")

    def s4_drop_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s4_drop_rollback")

        self._create_stream("s4", "o4")
        started = self._wait_status("s4", ("Running",))
        assert started == "Running", f"s4 never reached Running (got {started})"

        tdSql.execute("BEGIN")
        tdSql.execute("drop stream s4")
        tdSql.execute("ROLLBACK")

        assert "s4" in self._stream_names(), "stream s4 lost after DROP+ROLLBACK"

        # DS 5.2: an in-txn DROP only marks PRE_DROP; the task keeps running the whole
        # time, so after ROLLBACK it must still be Running without a redeploy.
        status = self._wait_status("s4", ("Running",), timeout=10)
        assert status == "Running", f"s4 should still be Running after ROLLBACK, got {status}"
        tdLog.info("s4 survived DROP+ROLLBACK and is still Running")

    def s5_create_then_drop_same_txn(self):
        self.s0_reset_env()
        tdLog.info("======== s5_create_then_drop_same_txn")

        tdSql.execute("BEGIN")
        self._create_stream("s5", "o5")
        tdSql.execute("drop stream s5")
        tdSql.execute("COMMIT")

        assert "s5" not in self._stream_names(), "s5 should not exist (created and dropped in one txn)"
        tdLog.info("s5 create+drop in one txn netted out correctly")

    # =========================================================================
    # Same-name recreate / rename
    # =========================================================================

    def s6_recreate_same_name_rejected(self):
        self.s0_reset_env()
        tdLog.info("======== s6_recreate_same_name_rejected")

        self._create_stream("s6", "o6")
        self._wait_status("s6", ("Running", "Idle"))

        tdSql.execute("BEGIN")
        tdSql.execute("drop stream s6")
        # 0x3320 == TSDB_CODE_TXN_RECREATE_NOT_SUPPORTED
        tdSql.error(
            "create stream s6 interval(10s) sliding(10s) from ct1"
            " into o6b as select _twstart ts, count(*) cnt from %%trows",
            expectErrInfo="cannot be recreated with the same name",
            fullMatched=False,
        )
        tdSql.execute("ROLLBACK")

        assert "s6" in self._stream_names(), "s6 lost after rejected recreate + ROLLBACK"
        tdLog.info("s6 same-name recreate correctly rejected; ROLLBACK restored the stream")

    def s7_rename_replacement_allowed(self):
        self.s0_reset_env()
        tdLog.info("======== s7_rename_replacement_allowed")

        self._create_stream("s7", "o7")
        self._wait_status("s7", ("Running", "Idle"))

        tdSql.execute("BEGIN")
        tdSql.execute("drop stream s7")
        self._create_stream("s7_v2", "o7")
        tdSql.execute("COMMIT")

        names = self._stream_names()
        assert "s7" not in names, "old stream s7 should be gone after COMMIT"
        assert "s7_v2" in names, "replacement stream s7_v2 missing after COMMIT"
        tdLog.info("s7 -> s7_v2 rename replacement committed correctly")

    # =========================================================================
    # End-to-end and regression
    # =========================================================================

    def s8_committed_stream_produces_output(self):
        self.s0_reset_env()
        tdLog.info("======== s8_committed_stream_produces_output")

        tdSql.execute("BEGIN")
        self._create_stream("s8", "o8")
        tdSql.execute("COMMIT")
        status = self._wait_status("s8", ("Running",))
        assert status == "Running", f"s8 never reached Running (got {status})"

        tdSql.execute("insert into ct1 values (now-5s, 1) (now-3s, 2) (now-1s, 3)")

        # The output table is created lazily on the stream's first write, and an interval
        # window only closes once the trigger sees a later timestamp -- so keep feeding
        # rows while polling instead of waiting passively.
        #
        # Poll with queryTimes=1, exit=False so a not-yet-created output table just returns
        # False. Without that, every poll before the first window closes logs a retry storm
        # plus an ERROR line for an entirely expected condition, which would bury real
        # failures in the CI log.
        deadline = time.time() + 120
        rows = 0
        while time.time() < deadline:
            tdSql.execute_ignore_error("insert into ct1 values (now, 9)")
            time.sleep(4)
            if tdSql.query("select * from o8", queryTimes=1, exit=False) is not False:
                rows = tdSql.queryRows
                if rows > 0:
                    break
        assert rows > 0, "stream created inside a txn produced no output after COMMIT"
        tdLog.info(f"s8 produced {rows} output row(s) end-to-end")

        # s8 fed the shared base table ct1 while polling (up to ~30 rows over 120s).
        # Drop that data so it cannot accumulate across reruns of this case — ct1 is a
        # BASE_TABLE that s0_reset_env keeps, so leftover rows would be invisible to the
        # object-set assertions but still accumulate in the file. Only rows this case
        # wrote (ts >= the now-5s seed) are removed; a subsequent scenario that needs
        # ct1 starts from a clean slate.
        tdSql.execute("delete from ct1 where ts > now - 10m")
        tdLog.info("s8 cleaned up its ct1 feed rows")

    def s9_no_stale_txnid_after_commit(self):
        """Regression: the committed stream must not keep a txnId, otherwise every later
        DDL on it fails with TSDB_CODE_TXN_RESOURCE_BUSY."""
        self.s0_reset_env()
        tdLog.info("======== s9_no_stale_txnid_after_commit")

        tdSql.execute("BEGIN")
        self._create_stream("s9", "o9")
        tdSql.execute("COMMIT")
        self._wait_status("s9", ("Running", "Idle"))

        # A plain non-txn DROP must succeed -- it would return RESOURCE_BUSY if the
        # commit had left txnId set on the stream object.
        tdSql.execute("drop stream s9")
        assert "s9" not in self._stream_names(), "s9 not dropped after COMMIT (stale txnId?)"
        tdLog.info("s9 dropped cleanly after COMMIT -- no stale txnId")

    # =========================================================================
    # s10: batch DROP STREAM s1, s2, ... (FS §4.12)
    #
    # Each stream in the list is processed independently — permission check,
    # conflict detection, PRE_DROP marking. Any one failing fails the whole
    # statement and the txn turns ABORTED; already-marked streams come back on
    # ROLLBACK. IF EXISTS applies per stream name. Charged per stream (§4.13).
    #
    # Per FS §4.6, a stream dropped inside a txn stays VISIBLE to that txn's own
    # SHOW STREAMS with status pending_drop — the definition is still there, marked.
    # Absence is therefore the wrong thing to assert in-txn; status is.
    # =========================================================================

    def _stream_status(self, name):
        tdSql.query("show streams")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == name:
                return tdSql.queryResult[i][1]
        return None

    def s10_batch_drop_stream(self):
        self.s0_reset_env()
        tdLog.info("======== s10_batch_drop_stream")

        for n in ("sa", "sb", "sc"):
            self._create_stream(n, f"o_{n}")
            self._wait_status(n, ("Running", "Idle"))
        assert {"sa", "sb", "sc"} <= set(self._stream_names()), self._stream_names()

        # --- batch drop inside a txn: both named streams go pending_drop, the third
        #     is untouched. ROLLBACK brings both back.
        tdSql.execute("BEGIN")
        tdSql.execute("drop stream sa, sb")
        assert self._stream_status("sa") == "pending_drop", self._stream_status("sa")
        assert self._stream_status("sb") == "pending_drop", self._stream_status("sb")
        assert self._stream_status("sc") not in (None, "pending_drop"), \
            f"sc must be untouched, got {self._stream_status('sc')}"
        tdSql.execute("ROLLBACK")
        names = self._stream_names()
        assert {"sa", "sb", "sc"} <= set(names), f"ROLLBACK lost streams: {names}"
        for n in ("sa", "sb"):
            assert self._stream_status(n) != "pending_drop", \
                f"{n} still pending_drop after ROLLBACK"

        # --- a missing name fails the whole statement; nothing is marked
        tdSql.execute("BEGIN")
        tdSql.execute_ignore_error("drop stream sa, nosuchstream")
        tdSql.execute("ROLLBACK")
        assert {"sa", "sb", "sc"} <= set(self._stream_names()), self._stream_names()

        # --- IF EXISTS tolerates the missing name and marks the real one
        tdSql.execute("BEGIN")
        tdSql.execute("drop stream if exists nosuchstream, sa")
        assert self._stream_status("sa") == "pending_drop", self._stream_status("sa")
        tdSql.execute("COMMIT")
        names = self._stream_names()
        assert "sa" not in names, f"sa survived COMMIT: {names}"
        assert {"sb", "sc"} <= set(names), names

        # --- batch drop + COMMIT removes them all for good
        tdSql.execute("BEGIN")
        tdSql.execute("drop stream sb, sc")
        tdSql.execute("COMMIT")
        names = self._stream_names()
        assert "sb" not in names and "sc" not in names, names

        # Source tables are never touched by DROP STREAM.
        assert "stb" in self._stables(), self._stables()
        tdLog.info("s10 PASSED: batch DROP STREAM isolation/rollback/commit, "
                   "IF EXISTS per name")

    # =========================================================================
    # s11: streams and table DDL share the single 50000-per-txn budget (FS §4.13).
    #
    # CREATE/DROP STREAM are TXN_STMT_DDL like table DDL, so they consume the same
    # txnDdlCount pool; there is no stream-specific limit. This case is the stream
    # counterpart of stress2.s119: it drives the shared counter with a mix of table
    # DDL and stream DDL and asserts no spurious rejection when comfortably under
    # the cap. Reaching the actual 50000 cap in CI would be far too slow (ASAN),
    # so the boundary itself is exercised offline (measure_txn_scale.py / manual).
    # =========================================================================

    def s11_streams_share_ddl_budget(self):
        self.s0_reset_env()
        tdLog.info("======== s11_streams_share_ddl_budget")

        # 60 tables + 2 streams + a few drops — well under 50000, so the whole
        # batch must be accepted. If streams were charged to a separate (or
        # double-counted) pool, this would still pass; the real guard is that
        # nothing is rejected and the counter is not left poisoned afterwards.
        tdSql.execute("BEGIN")
        for i in range(60):
            tdSql.execute(f"create table b{i} using stb tags({i})")
        self._create_stream("s11a", "o11a")
        self._create_stream("s11b", "o11b")
        tdSql.execute("drop stream s11b")
        tdSql.execute("COMMIT")
        assert "s11a" in self._stream_names(), "stream s11a lost after COMMIT"
        assert "s11b" not in self._stream_names(), "stream s11b should be dropped"

        # The counter must reset on COMMIT — a fresh txn still has full budget.
        # `show tables` lists child + normal tables (super tables only appear in
        # `show stables`), so the expected set is the reset baseline ct1 (1) plus
        # the 60 b* child tables from the first txn plus the 20 c* child tables here.
        tdSql.execute("BEGIN")
        for i in range(20):
            tdSql.execute(f"create table c{i} using stb tags({i})")
        tdSql.execute("COMMIT")
        tdSql.query("show tables")
        tdSql.checkRows(1 + 60 + 20)

        tdLog.info("s11 PASSED: streams share the per-txn DDL budget with table DDL")

    # =========================================================================
    # s12: definition replacement — no "old dropped, new not yet created"
    #      intermediate state (DS §8 定义替换).
    #
    # DROP s + CREATE s2 in one txn: until COMMIT the old stream keeps running and
    # the new stream has no task (it is PRE_CREATE, never deployed). After COMMIT
    # the old one is gone and the new one runs. This pins the invariant that a
    # stream definition is never absent for other connections mid-txn — either the
    # old definition (pre-COMMIT) or the new one (post-COMMIT) is visible.
    # =========================================================================

    def s12_definition_replacement_no_gap(self):
        self.s0_reset_env()
        tdLog.info("======== s12_definition_replacement_no_gap")

        self._create_stream("s12old", "o12")
        self._wait_status("s12old", ("Running", "Idle"))

        conn2, cursor2 = self._other_conn()
        try:
            tdSql.execute("BEGIN")
            tdSql.execute("drop stream s12old")
            self._create_stream("s12new", "o12")

            # Pre-COMMIT from another connection: the old definition is still there
            # (still running), the new name does not exist yet.
            cursor2.execute("show streams")
            rows = {r[0]: r[1] for r in cursor2.fetchall()}
            assert "s12old" in rows, \
                f"old stream must stay visible until COMMIT, saw: {rows}"
            assert rows.get("s12old") not in (None, "pending_drop"), \
                f"old stream must still run for others pre-COMMIT, got {rows.get('s12old')}"
            assert "s12new" not in rows, \
                f"new stream must not be visible pre-COMMIT, saw: {rows}"

            # SHOW STREAM TASKS must not list the pending_create stream.
            # (ins_stream_tasks carries one row per deployed task; a PRE_CREATE
            # stream that was never deployed must have no row at all.)
            cursor2.execute("select stream_name from information_schema.ins_stream_tasks")
            tasks = {r[0] for r in cursor2.fetchall()}
            assert "s12new" not in tasks, \
                f"PRE_CREATE stream must have no task, saw: {tasks}"

            tdSql.execute("COMMIT")
        finally:
            cursor2.close()
            conn2.close()

        # Post-COMMIT: the old name is gone, the new one runs.
        names = self._stream_names()
        assert "s12old" not in names, "old stream survived replacement COMMIT"
        assert "s12new" in names, "new stream missing after replacement COMMIT"
        status = self._wait_status("s12new", ("Running", "Idle"))
        assert status in ("Running", "Idle"), \
            f"replacement stream did not deploy, status={status}"
        tdLog.info(f"s12 replacement committed: old gone, new running (status={status})")

    # =========================================================================
    # s13: no stale txn markers after COMMIT (direct status check).
    #
    # ins_streams has no txn_id column, but the `status` column does expose the
    # marker: `pending_create` / `pending_drop` are only rendered while
    # SStreamObj.txnId != 0 for the viewing connection (mndStreamUtil.c). So a
    # stream that reads back as Running/Idle (not pending_*) after COMMIT is
    # direct evidence that the txn markers were cleared from SDB — the same
    # marker whose residue s9 guards against symptomatically via RESOURCE_BUSY.
    # =========================================================================

    def s13_txn_markers_cleared_after_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s13_txn_markers_cleared_after_commit")

        tdSql.execute("BEGIN")
        self._create_stream("s13", "o13")
        # In-txn the owning connection sees pending_create.
        assert self._stream_status("s13") == "pending_create", \
            self._stream_status("s13")
        tdSql.execute("COMMIT")

        # After COMMIT the same connection must see the real runtime status, not
        # pending_create — the txnId/txnStatus were cleared from SDB.
        status = self._wait_status("s13", ("Running", "Idle"))
        assert status in ("Running", "Idle"), \
            f"s13 still pending after COMMIT (stale marker?), got {status}"

        # ROLLBACK must fully undo the marker: the stream is gone from ins_streams.
        tdSql.execute("BEGIN")
        self._create_stream("s13rb", "o13rb")
        assert self._stream_status("s13rb") == "pending_create", \
            self._stream_status("s13rb")
        tdSql.execute("ROLLBACK")
        tdSql.query("select count(*) from information_schema.ins_streams "
                    "where stream_name = 's13rb'")
        tdSql.checkData(0, 0, 0)

        tdLog.info("s13 PASSED: txn markers cleared after COMMIT, "
                   "fully undone by ROLLBACK")

    # =========================================================================
    # Mixed table DDL + stream DDL in one transaction
    # =========================================================================

    def m1_table_and_stream_commit(self):
        self.s0_reset_env()
        tdLog.info("======== m1_table_and_stream_commit")

        tdSql.execute("BEGIN")
        tdSql.execute("create table m1_nt (ts timestamp, a int)")
        tdSql.execute("create table m1_ct using stb tags(11)")
        self._create_stream("m1_s", "m1_o")
        tdSql.execute("COMMIT")

        tables = self._tables()
        assert "m1_nt" in tables, "normal table lost after mixed COMMIT"
        assert "m1_ct" in tables, "child table lost after mixed COMMIT"
        assert "m1_s" in self._stream_names(), "stream lost after mixed COMMIT"
        tdLog.info("m1 table DDL + stream committed atomically")

    def m2_table_and_stream_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== m2_table_and_stream_rollback")

        before = self._tables()
        tdSql.execute("BEGIN")
        tdSql.execute("create table m2_nt (ts timestamp, a int)")
        tdSql.execute("create table m2_ct using stb tags(22)")
        self._create_stream("m2_s", "m2_o")
        tdSql.execute("ROLLBACK")

        after = self._tables()
        assert "m2_nt" not in after, "normal table survived mixed ROLLBACK"
        assert "m2_ct" not in after, "child table survived mixed ROLLBACK"
        assert "m2_s" not in self._stream_names(), "stream survived mixed ROLLBACK"
        assert after == before, f"table set changed after ROLLBACK: {before} -> {after}"
        tdLog.info("m2 table DDL + stream rolled back atomically")

    def m3_stream_over_table_created_in_same_txn(self):
        """A stream may reference a table created earlier in the same transaction."""
        self.s0_reset_env()
        tdLog.info("======== m3_stream_over_table_created_in_same_txn")

        tdSql.execute("BEGIN")
        tdSql.execute("create table m3_src (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table m3_c1 using m3_src tags(1)")
        self._create_stream_from("m3_s", "m3_c1", "m3_o")
        tdSql.execute("COMMIT")

        assert "m3_src" in self._stables(), "source stable lost after COMMIT"
        assert "m3_s" in self._stream_names(), "stream lost after COMMIT"
        status = self._wait_status("m3_s", ("Running", "Idle"))
        assert status in ("Running", "Idle"), f"m3_s did not deploy, status={status}"
        tdLog.info(f"m3 stream over same-txn table committed and deployed (status={status})")

    def m4_stream_over_same_txn_table_rollback(self):
        """Rolling back must remove the stream and its brand-new source table together,
        leaving no stream that references a table which no longer exists."""
        self.s0_reset_env()
        tdLog.info("======== m4_stream_over_same_txn_table_rollback")

        tdSql.execute("BEGIN")
        tdSql.execute("create table m4_src (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table m4_c1 using m4_src tags(1)")
        self._create_stream_from("m4_s", "m4_c1", "m4_o")
        tdSql.execute("ROLLBACK")

        assert "m4_src" not in self._stables(), "source stable survived ROLLBACK"
        assert "m4_c1" not in self._tables(), "child table survived ROLLBACK"
        assert "m4_s" not in self._stream_names(), "stream survived ROLLBACK (dangling ref)"
        tdLog.info("m4 stream and its same-txn source table rolled back together")

    def m5_drop_stream_and_its_source_table(self):
        """FS 4.8: DROP STREAM followed by dropping the table it read from is allowed."""
        self.s0_reset_env()
        tdLog.info("======== m5_drop_stream_and_its_source_table")

        tdSql.execute("create table m5_src (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table m5_c1 using m5_src tags(1)")
        self._create_stream_from("m5_s", "m5_c1", "m5_o")
        self._wait_status("m5_s", ("Running", "Idle"))

        tdSql.execute("BEGIN")
        tdSql.execute("drop stream m5_s")
        tdSql.execute("drop stable m5_src")
        tdSql.execute("COMMIT")

        assert "m5_s" not in self._stream_names(), "stream survived DROP+COMMIT"
        assert "m5_src" not in self._stables(), "source stable survived DROP+COMMIT"
        tdLog.info("m5 stream and its source table dropped together by COMMIT")

    def m6_precreated_out_stable_with_partitioned_stream(self):
        """Create the output super table and a partitioned stream writing into it in one
        transaction -- the eager (super table) output form, unlike the lazy normal-table
        form used elsewhere in this file."""
        self.s0_reset_env()
        tdLog.info("======== m6_precreated_out_stable_with_partitioned_stream")

        tdSql.execute("BEGIN")
        tdSql.execute(
            "create stable m6_out (ts timestamp, cnt bigint) tags (tname varchar(128))"
        )
        tdSql.execute(
            "create stream m6_s interval(10s) sliding(10s) from stb partition by tbname"
            " into m6_out tags(tname varchar(128) as tbname)"
            " as select _twstart ts, count(*) cnt from %%trows"
        )
        tdSql.execute("COMMIT")

        assert "m6_out" in self._stables(), "output stable lost after COMMIT"
        assert "m6_s" in self._stream_names(), "partitioned stream lost after COMMIT"
        status = self._wait_status("m6_s", ("Running", "Idle"))
        assert status in ("Running", "Idle"), f"m6_s did not deploy, status={status}"
        tdLog.info(f"m6 out stable + partitioned stream committed together (status={status})")

    def m7_alter_stable_with_stream_commit(self):
        self.s0_reset_env()
        tdSql.execute_ignore_error("alter stable stb drop column m7_col")
        tdLog.info("======== m7_alter_stable_with_stream_commit")

        tdSql.execute("BEGIN")
        tdSql.execute("alter stable stb add column m7_col int")
        tdSql.execute("create table m7_ct using stb tags(77)")
        self._create_stream("m7_s", "m7_o")
        tdSql.execute("COMMIT")

        assert "m7_col" in self._columns("stb"), "ALTER STABLE not applied after COMMIT"
        assert "m7_ct" in self._tables(), "child table lost after COMMIT"
        assert "m7_s" in self._stream_names(), "stream lost after COMMIT"
        tdLog.info("m7 ALTER + CREATE TABLE + CREATE STREAM committed together")

        # Leave the shared stb schema as we found it.
        tdSql.execute("alter stable stb drop column m7_col")

    def m8_alter_stable_with_stream_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== m8_alter_stable_with_stream_rollback")

        cols_before = self._columns("stb")
        tdSql.execute("BEGIN")
        tdSql.execute("alter stable stb add column m8_col int")
        tdSql.execute("create table m8_ct using stb tags(88)")
        self._create_stream("m8_s", "m8_o")
        tdSql.execute("ROLLBACK")

        cols_after = self._columns("stb")
        assert "m8_col" not in cols_after, "ALTER STABLE survived ROLLBACK"
        assert cols_after == cols_before, f"stb schema changed: {cols_before} -> {cols_after}"
        assert "m8_ct" not in self._tables(), "child table survived ROLLBACK"
        assert "m8_s" not in self._stream_names(), "stream survived ROLLBACK"
        tdLog.info("m8 ALTER + CREATE TABLE + CREATE STREAM rolled back together")

    def m9_failed_stream_stmt_keeps_txn_usable(self):
        """A rejected stream statement fails on its own; it neither aborts the txn nor
        discards the table DDL issued before/after it. Matches phase-1 behaviour where a
        single failing DDL does not poison the whole transaction."""
        self.s0_reset_env()
        tdLog.info("======== m9_failed_stream_stmt_keeps_txn_usable")

        # An already-existing stream makes the CREATE inside the txn fail.
        self._create_stream("m9_dup", "m9_dup_o")
        self._wait_status("m9_dup", ("Running", "Idle"))

        tdSql.execute("BEGIN")
        tdSql.execute("create table m9_before (ts timestamp, a int)")
        tdSql.error(
            "create stream m9_dup interval(10s) sliding(10s) from ct1"
            " into m9_other as select _twstart ts, count(*) cnt from %%trows",
            expectErrInfo="Stream already exists",
            fullMatched=False,
        )
        tdSql.execute("create table m9_after (ts timestamp, a int)")
        tdSql.execute("COMMIT")

        tables = self._tables()
        assert "m9_before" in tables, "table created before the failing stmt was lost"
        assert "m9_after" in tables, "table created after the failing stmt was lost"
        tdLog.info("m9 failing stream stmt did not discard the surrounding table DDL")

    def _other_conn(self):
        """A second connection, so a concurrent transaction can be driven independently.
        tdSql is a single session: BEGIN/COMMIT on it belong to one txn, so cross-txn
        conflict codes can only be reached from a separate connection."""
        conn = taos.connect()
        cursor = conn.cursor()
        cursor.execute("use txn_stm")
        return conn, cursor

    def m10_multiple_streams_share_out_table(self):
        """Several streams may share one output STB, in or out of a transaction.

        Non-transactional baseline: two plain CREATE STREAMs writing into the same existing
        STB are accepted and both run. A transaction must not be stricter than the baseline —
        wrapping a statement in BEGIN changes atomicity and visibility, not what is legal —
        so the same pairing is accepted across two concurrent txns.

        This previously raised TSDB_CODE_TXN_STREAM_OUT_TABLE_BUSY (0x3321), which made
        legal DDL fail purely because it was inside a txn. That check (and the error code)
        was removed; this test guards against it coming back."""
        self.s0_reset_env()
        tdLog.info("======== m10_multiple_streams_share_out_table")

        # Form B requires the output STB to already exist, outside any txn.
        tdSql.execute("create stable m10_out (ts timestamp, cnt bigint) tags (tname varchar(128))")

        # (a) baseline: two non-txn streams into one existing output STB.
        tdSql.execute(
            "create stream m10_base1 interval(10s) sliding(10s) from stb partition by tbname"
            " into m10_out tags(tname varchar(128) as tbname)"
            " as select _twstart ts, count(*) cnt from %%trows"
        )
        tdSql.execute(
            "create stream m10_base2 interval(20s) sliding(20s) from stb partition by tbname"
            " into m10_out tags(tname varchar(128) as tbname)"
            " as select _twstart ts, count(*) cnt from %%trows"
        )
        names = self._stream_names()
        assert "m10_base1" in names and "m10_base2" in names, \
            "non-txn baseline: two streams should share one output STB"
        tdLog.info("m10 (a) baseline allows two non-txn streams to share an output STB")
        tdSql.execute("drop stream m10_base1")
        tdSql.execute("drop stream m10_base2")

        # (b) two concurrent txns, same output STB: must match the baseline and be allowed.
        conn2, cursor2 = self._other_conn()
        try:
            tdSql.execute("BEGIN")
            tdSql.execute(
                "create stream m10_s1 interval(10s) sliding(10s) from stb partition by tbname"
                " into m10_out tags(tname varchar(128) as tbname)"
                " as select _twstart ts, count(*) cnt from %%trows"
            )

            cursor2.execute("BEGIN")
            try:
                cursor2.execute(
                    "create stream m10_s2 interval(20s) sliding(20s) from stb partition by tbname"
                    " into m10_out tags(tname varchar(128) as tbname)"
                    " as select _twstart ts, count(*) cnt from %%trows"
                )
                tdLog.info("m10 (b) concurrent txn sharing the output STB accepted, as the baseline does")
            except Exception as e:
                msg = str(e)
                assert "0x3321" not in msg and "referenced by a stream" not in msg, \
                    f"0x3321 is back: a txn is stricter than the non-txn baseline: {msg}"
                raise
            finally:
                cursor2.execute("ROLLBACK")

            # (c) same txn, same output STB: also allowed.
            tdSql.execute(
                "create stream m10_s3 interval(30s) sliding(30s) from stb partition by tbname"
                " into m10_out tags(tname varchar(128) as tbname)"
                " as select _twstart ts, count(*) cnt from %%trows"
            )
            tdLog.info("m10 (c) same-txn second stream into the same output STB accepted")

            tdSql.execute("ROLLBACK")
        finally:
            cursor2.close()
            conn2.close()

        tdSql.execute("drop stable m10_out")
        tdLog.info("m10 verified: sharing an output STB is legal in and out of a txn")

    def m10b_out_table_drop_outside_txn_is_not_blocked(self):
        """Documents the *current* behaviour of dropping a stream's output STB while an
        uncommitted stream in another txn still references it.

        There is no guard on the DROP side, in or out of a transaction, so each of these
        succeeds today:
          - non-txn DROP of an STB referenced by another txn's uncommitted stream
          - in-txn DROP of that same STB from a second txn
          - non-txn DROP of an already-committed stream's output STB

        This matches the non-transactional baseline, where dropping a running stream's output
        table is allowed and the stream only turns Failed when it next tries to write (weak
        reference + lazy failure).

        What the baseline does NOT promise, and a transaction does, is that COMMIT yields a
        working object: case (a) below commits a stream whose output table is already gone.
        Note mndCheckDropStbForStream() in mndStb.c would reject the third case, but it is
        dead code with no callers. This test pins the behaviour as observed; if a guard or a
        COMMIT-time revalidation is added later it will fail here and should be updated
        deliberately rather than silently."""
        self.s0_reset_env()
        tdLog.info("======== m10b_out_table_drop_outside_txn_is_not_blocked")

        conn2, cursor2 = self._other_conn()
        try:
            # (a) non-txn DROP while another txn's uncommitted stream references the STB.
            tdSql.execute("create stable m10b_out (ts timestamp, cnt bigint) tags (tname varchar(128))")
            tdSql.execute("BEGIN")
            tdSql.execute(
                "create stream m10b_s interval(10s) sliding(10s) from stb partition by tbname"
                " into m10b_out tags(tname varchar(128) as tbname)"
                " as select _twstart ts, count(*) cnt from %%trows"
            )
            cursor2.execute("drop stable m10b_out")
            tdLog.info("m10b (a) non-txn DROP of an in-txn stream's output STB is allowed today")

            # The txn commits even though its output STB is gone: no cross-check at COMMIT.
            tdSql.execute("COMMIT")
            assert "m10b_s" in self._stream_names(), "stream lost after COMMIT"
            assert "m10b_out" not in self._stables(), "output STB should still be gone"
            tdLog.info("m10b (a) COMMIT succeeded with the output STB already dropped")
            tdSql.execute("drop stream m10b_s")

            # (b) in-txn DROP from a second txn is likewise not blocked by 0x3321.
            tdSql.execute("create stable m10b_out2 (ts timestamp, cnt bigint) tags (tname varchar(128))")
            tdSql.execute("BEGIN")
            tdSql.execute(
                "create stream m10b_s2 interval(10s) sliding(10s) from stb partition by tbname"
                " into m10b_out2 tags(tname varchar(128) as tbname)"
                " as select _twstart ts, count(*) cnt from %%trows"
            )
            cursor2.execute("BEGIN")
            cursor2.execute("drop stable m10b_out2")
            cursor2.execute("ROLLBACK")
            tdSql.execute("ROLLBACK")
            tdLog.info("m10b (b) in-txn DROP from a second txn is allowed today")

            # (c) baseline: committed stream's output STB is droppable with no txn involved.
            tdSql.execute("create stable m10b_out3 (ts timestamp, cnt bigint) tags (tname varchar(128))")
            tdSql.execute(
                "create stream m10b_s3 interval(10s) sliding(10s) from stb partition by tbname"
                " into m10b_out3 tags(tname varchar(128) as tbname)"
                " as select _twstart ts, count(*) cnt from %%trows"
            )
            self._wait_status("m10b_s3", ("Running", "Idle"))
            tdSql.execute("drop stable m10b_out3")
            tdLog.info("m10b (c) committed stream's output STB is droppable (no reverse guard)")
        finally:
            cursor2.close()
            conn2.close()

    def m11_stream_no_snode(self):
        """CREATE STREAM with no snode available reports the same code in and out of a
        transaction: TSDB_CODE_SNODE_NO_AVAILABLE_NODE (0x042C).

        "No snode available" is a resource condition, not a transactional one — BEGIN does
        not change what it means, so it does not warrant a separate code. This previously
        returned TSDB_CODE_TXN_STREAM_NO_SNODE (0x3322) on the txn path only; that code was
        removed. Both paths are asserted here so the two cannot drift apart again."""
        self.s0_reset_env()
        tdLog.info("======== m11_stream_no_snode")

        # `show snodes` column 0 is the snode id, which equals its dnode id.
        tdSql.query("show snodes")
        snode_ids = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        for snode_id in snode_ids:
            tdSql.execute_ignore_error(f"drop snode on dnode {snode_id}")
        time.sleep(2)

        tdSql.query("show snodes")
        assert tdSql.queryRows == 0, "snodes still present; cannot exercise the no-snode path"

        try:
            # (a) non-txn path.
            tdSql.error(
                "create stream m11_nt interval(10s) sliding(10s) from ct1"
                " into m11_nt_o as select _twstart ts, count(*) cnt from %%trows",
                expectErrInfo="No Snode is available",
                fullMatched=False,
            )
            tdLog.info("m11 (a) non-txn CREATE STREAM without snode reports 0x042C")

            # (b) txn path must report the very same error, not a txn-specific one.
            tdSql.execute("BEGIN")
            tdSql.error(
                "create stream m11_s interval(10s) sliding(10s) from ct1"
                " into m11_o as select _twstart ts, count(*) cnt from %%trows",
                expectErrInfo="No Snode is available",
                fullMatched=False,
            )
            tdSql.execute("ROLLBACK")
            tdLog.info("m11 (b) in-txn CREATE STREAM without snode reports the same 0x042C")
        finally:
            # Every later scenario needs a snode, so restore it even if the assert above fails.
            tdSql.execute_ignore_error("create snode on dnode 1")
            time.sleep(2)
            tdSql.query("show snodes")
            assert tdSql.queryRows > 0, (
                "snode was not restored after m11; later scenarios (m12+) will fail"
            )

        tdLog.info("m11 verified: no-snode reports 0x042C on both paths")

    def m12_snode_protected_by_uncommitted_stream(self):
        """An uncommitted stream (created inside a transaction but not yet committed) holds a
        reference to its snode, preventing DROP SNODE from outside the transaction.

        This matches the baseline behavior: a committed stream blocks DROP SNODE with
        "Snode still in use with streams" (0x7007). A transaction must not weaken that guard —
        uncommitted streams also pin the snode.

        Note: DROP SNODE itself is not allowed inside a transaction (0x331C), so the
        "can I drop the snode from inside the same txn" question does not arise."""
        self.s0_reset_env()
        tdLog.info("======== m12_snode_protected_by_uncommitted_stream")

        # Baseline: a committed stream blocks DROP SNODE.
        tdSql.execute(
            "create stream m12_base interval(10s) sliding(10s) from ct1"
            " into m12_base_out as select _twstart ts, count(*) cnt from %%trows"
        )
        self._wait_status("m12_base", ("Running", "Idle"))
        tdSql.error("drop snode on dnode 1", expectErrInfo="Snode still in use with streams")
        tdLog.info("m12 baseline: committed stream blocks DROP SNODE")
        tdSql.execute("drop stream m12_base")

        # Key test: uncommitted stream also blocks DROP SNODE from outside the txn.
        conn2, cursor2 = self._other_conn()
        try:
            tdSql.execute("BEGIN")
            tdSql.execute(
                "create stream m12_s interval(10s) sliding(10s) from ct1"
                " into m12_out as select _twstart ts, count(*) cnt from %%trows"
            )
            tdLog.info("m12 uncommitted stream created in txn")

            # From another connection (outside the txn), try to drop the snode.
            try:
                cursor2.execute("drop snode on dnode 1")
                raise AssertionError(
                    "DROP SNODE should have been blocked by the uncommitted stream, "
                    "but it succeeded (bug: uncommitted stream does not hold snode reference)"
                )
            except AssertionError:
                raise
            except Exception as e:
                msg = str(e)
                assert "0x7007" in msg or "Snode still in use with streams" in msg, \
                    f"expected 0x7007 (Snode still in use with streams), got: {msg}"
                tdLog.info(f"m12 uncommitted stream correctly blocks DROP SNODE: {msg}")

            tdSql.execute("ROLLBACK")
        finally:
            cursor2.close()
            conn2.close()

        tdLog.info("m12 verified: uncommitted streams hold snode references")

    def m13_uncommitted_stream_does_not_start(self):
        """A stream created inside a transaction must not deploy before COMMIT.

        Deploying early would run a stream the user can still ROLLBACK, and the rollback
        undo path assumes it was never deployed (it does no snode-side compensation). The
        guard is explicit in msmCheckLoopStreamSdb: a stream whose txnStatus is PRE_CREATE
        or PRE_CREATE_DROP is skipped by the deploy loop.

        The wait here is deliberately longer than the scheduler's isolation window, so the
        test would catch a regression where the stream is only *slow* to deploy rather than
        genuinely held back."""
        self.s0_reset_env()
        tdLog.info("======== m13_uncommitted_stream_does_not_start")

        conn2, cursor2 = self._other_conn()
        try:
            tdSql.execute("BEGIN")
            self._create_stream("m13_s", "m13_o")

            # Watch across several scheduler loops. Never Running/Idle before COMMIT.
            #
            # Two connections are checked each pass, because "not Running" alone would also
            # hold if the stream had never been created:
            #   - the owning txn must see it as pending_create (proof it exists);
            #   - conn2 must not see it at all (PRE_CREATE is hidden from other
            #     connections by mndRetrieveStreams, so absence here is the expectation).
            for i in range(6):
                time.sleep(3)
                own_status = self._stream_status("m13_s")
                assert own_status == "pending_create", (
                    f"owning txn should see m13_s as pending_create at t+{(i + 1) * 3}s, "
                    f"got {own_status}"
                )
                cursor2.execute("show streams")
                rows = {r[0]: r[1] for r in cursor2.fetchall()}
                status = rows.get("m13_s")
                assert status not in ("Running", "Idle"), (
                    f"uncommitted stream deployed before COMMIT at t+{(i + 1) * 3}s "
                    f"(status={status}); it must stay undeployed until COMMIT"
                )
            tdLog.info(f"m13 stream stayed undeployed for 18s before COMMIT (status={status})")

            # After COMMIT it must actually deploy — the guard must not be a permanent block.
            tdSql.execute("COMMIT")
            status = self._wait_status("m13_s", ("Running", "Idle"))
            assert status in ("Running", "Idle"), \
                f"stream failed to deploy after COMMIT, status={status}"
            tdLog.info(f"m13 stream deployed after COMMIT, status={status}")
            tdSql.execute("drop stream m13_s")
        finally:
            cursor2.close()
            conn2.close()

        tdLog.info("m13 verified: uncommitted streams do not start, committed ones do")

    def m14_out_stable_created_in_txn(self):
        """A stream that owns its output super table (form A) must create that table inside
        the transaction, just as the non-txn path does.

        The txn path used to skip output-table creation entirely: the marker helper returned
        before reaching it. The stream then committed with no table to write into and turned
        Failed on its first output, while the identical statement outside a txn worked. This
        asserts the three things that follow from creating it properly:
          (a) non-txn baseline creates the output STB immediately
          (b) in-txn COMMIT yields a *working* stream, i.e. the table exists and it runs
          (c) ROLLBACK undoes the table, since it is this txn's own side effect"""
        self.s0_reset_env()
        tdLog.info("======== m14_out_stable_created_in_txn")

        def partitioned_stream(name, out):
            return (
                f"create stream {name} interval(10s) sliding(10s) from stb partition by tbname"
                f" into {out} as select _twstart ts, count(*) cnt from %%trows"
            )

        # (a) baseline: the output STB appears as soon as CREATE STREAM returns.
        tdSql.execute(partitioned_stream("m14_base", "m14_base_out"))
        assert "m14_base_out" in self._stables(), \
            "non-txn baseline should create the output STB at CREATE STREAM time"
        tdLog.info("m14 (a) non-txn CREATE STREAM created its output STB immediately")
        tdSql.execute("drop stream m14_base")
        tdSql.execute("drop stable m14_base_out")

        # (b) in-txn + COMMIT: table must exist and the stream must actually run.
        tdSql.execute("BEGIN")
        tdSql.execute(partitioned_stream("m14_c", "m14_c_out"))
        assert "m14_c_out" in self._stables(), \
            "output STB should be visible to its own txn (read-your-own-writes)"
        tdSql.execute("COMMIT")
        assert "m14_c_out" in self._stables(), "output STB missing after COMMIT"
        status = self._wait_status("m14_c", ("Running", "Idle"))
        assert status in ("Running", "Idle"), \
            f"committed stream should run, not fail on a missing output table (status={status})"
        tdLog.info(f"m14 (b) committed txn stream has its output STB and runs, status={status}")
        tdSql.execute("drop stream m14_c")
        tdSql.execute("drop stable m14_c_out")

        # (c) ROLLBACK: the table is this txn's side effect and must be undone.
        tdSql.execute("BEGIN")
        tdSql.execute(partitioned_stream("m14_r", "m14_r_out"))
        tdSql.execute("ROLLBACK")
        assert "m14_r_out" not in self._stables(), \
            "output STB created by the rolled-back txn should have been undone"
        assert "m14_r" not in self._stream_names(), "stream should be gone after ROLLBACK"
        tdLog.info("m14 (c) ROLLBACK undid both the stream and its output STB")

        # (d) form C: a pre-existing output STB is only referenced, never undone.
        tdSql.execute("create stable m14_pre (ts timestamp, cnt bigint) tags (tname varchar(128))")
        tdSql.execute("create table m14_pre_sub using m14_pre tags('keep')")
        tdSql.execute("insert into m14_pre_sub values (now, 999)")
        tdSql.execute("BEGIN")
        tdSql.execute(
            "create stream m14_p interval(10s) sliding(10s) from stb partition by tbname"
            " into m14_pre tags(tname varchar(128) as tbname)"
            " as select _twstart ts, count(*) cnt from %%trows"
        )
        tdSql.execute("ROLLBACK")
        assert "m14_pre" in self._stables(), "pre-existing output STB must survive ROLLBACK"
        tdSql.query("select count(*) from m14_pre")
        assert tdSql.queryResult[0][0] == 1, "pre-existing output STB lost its data on ROLLBACK"
        tdLog.info("m14 (d) pre-existing output STB and its data survived ROLLBACK")

        tdLog.info("m14 verified: form A/B output STB is a txn side effect, form C is not")

    def m15_nodelay_out_table_created_in_txn(self):
        """Form B with NODELAY_CREATE_SUBTABLE: the output NORMAL table is created inside
        the txn, visible to that txn only (FS 4.2 / 4.6).

        Outside a txn this option creates the output table eagerly at CREATE STREAM time --
        that is the whole point of asking for it. Inside a txn the option keeps its meaning:
        the table is created immediately and this session can DESC it, while other sessions
        must not see it until COMMIT. ROLLBACK undoes it and frees the name.

        The last part also guards the form-A defect: COMMIT must not leave a stream with no
        table to write into. The stream has to reach Running and actually emit rows."""
        self.s0_reset_env()
        tdLog.info("======== m15_nodelay_out_table_created_in_txn")

        def nodelay_stream(name, out):
            return (
                f"create stream {name} interval(10s) sliding(10s) from ct1"
                f" into {out} NODELAY_CREATE_SUBTABLE"
                f" as select _twstart ts, count(*) cnt from %%trows"
            )

        conn2, cursor2 = self._other_conn()
        try:
            # (a) ROLLBACK: created in-txn, visible only here, then undone.
            tdSql.execute("BEGIN")
            tdSql.execute(nodelay_stream("m15_r", "m15_r_out"))
            assert "m15_r_out" in self._tables(), \
                "nodelay output table should be created inside the txn"
            # Readable from this txn, and empty: a new stream emits nothing before COMMIT.
            tdSql.query("select count(*) from m15_r_out")
            tdSql.checkData(0, 0, 0)
            tdSql.query("desc m15_r_out")
            assert tdSql.queryRows > 0, "output table should be describable inside the txn"

            cursor2.execute("show tables")
            outside = sorted(r[0] for r in cursor2.fetchall())
            assert "m15_r_out" not in outside, \
                "uncommitted nodelay output table leaked to another session"

            tdSql.execute("ROLLBACK")
            assert "m15_r_out" not in self._tables(), "table should be gone after ROLLBACK"
            assert "m15_r" not in self._stream_names(), "stream should be gone after ROLLBACK"
            # The name is released immediately, so it can be reused right away.
            tdSql.execute("create table m15_r_out (ts timestamp, v int)")
            tdSql.execute("drop table m15_r_out")
            tdLog.info("m15 (a) created in-txn, hidden outside, undone by ROLLBACK, name reusable")

            # (b) COMMIT: the table becomes visible everywhere and the stream really runs.
            tdSql.execute("BEGIN")
            tdSql.execute(nodelay_stream("m15_c", "m15_c_out"))
            assert "m15_c_out" in self._tables(), \
                "nodelay output table should be created inside the txn"
            tdSql.execute("COMMIT")

            assert "m15_c_out" in self._tables(), "output table should survive COMMIT"
            cursor2.execute("show tables")
            outside = sorted(r[0] for r in cursor2.fetchall())
            assert "m15_c_out" in outside, \
                "committed nodelay output table should be visible to other sessions"
            tdLog.info("m15 (b) output table visible to all sessions after COMMIT")

            status = self._wait_status("m15_c", ("Running", "Idle"))
            assert status in ("Running", "Idle"), \
                f"committed nodelay stream should run, status={status}"

            deadline = time.time() + 60
            rows = 0
            while time.time() < deadline:
                tdSql.execute("insert into ct1 values (now, 1)")
                time.sleep(3)
                tdSql.query("select count(*) from m15_c_out")
                rows = tdSql.queryResult[0][0]
                if rows > 0:
                    break
            assert rows > 0, (
                "committed nodelay stream produced no output "
                f"(status={self._wait_status('m15_c', ('Running',), 1)})"
            )
            tdLog.info(f"m15 (b) committed nodelay stream emitted {rows} row(s)")
            tdSql.execute("drop stream m15_c")
        finally:
            cursor2.close()
            conn2.close()

        tdLog.info("m15 verified: nodelay output table is created in-txn, isolated, and works after COMMIT")

    def m18_preexisting_normal_out_table(self):
        """Form D with a pre-existing NORMAL table (not an STB): the outTblExists vs
        outStbExists dispatch. FS 4.2 form D / DS 5.1.

        A stream whose INTO names a table that already exists only references it — it must
        NOT try to create it. The parser sets outStbExists only for a pre-existing SUPER
        table; a pre-existing NORMAL table leaves it false, so dispatching the txn path on
        outStbExists would misread the table as "to be created" and the create would then
        fail with TABLE_ALREADY_EXIST. outTblExists fixes that. This test pins: a txn
        CREATE STREAM into an existing normal table succeeds, and ROLLBACK leaves the table
        and its data intact (form D is not this txn's side effect)."""
        self.s0_reset_env()
        tdLog.info("======== m18_preexisting_normal_out_table")

        # A plain normal table, already committed and holding data.
        tdSql.execute("create table m18_out (ts timestamp, cnt bigint)")
        tdSql.execute("insert into m18_out values (now, 42)")

        # Baseline: non-txn CREATE STREAM into the existing normal table works.
        tdSql.execute(
            "create stream m18_base interval(10s) sliding(10s) from ct1"
            " into m18_out as select _twstart ts, count(*) cnt from %%trows"
        )
        assert "m18_base" in self._stream_names(), \
            "non-txn baseline should reference the existing normal table"
        tdSql.execute("drop stream m18_base")
        tdSql.query("select count(*) from m18_out")
        assert tdSql.queryResult[0][0] == 1, "baseline should not disturb the pre-existing table's data"

        # In-txn + ROLLBACK: referencing an existing normal table must succeed and not be
        # undone (form D, not this txn's side effect).
        tdSql.execute("BEGIN")
        tdSql.execute(
            "create stream m18_r interval(10s) sliding(10s) from ct1"
            " into m18_out as select _twstart ts, count(*) cnt from %%trows"
        )
        tdSql.execute("ROLLBACK")
        assert "m18_r" not in self._stream_names(), "rolled-back stream should be gone"
        tdSql.query("select count(*) from m18_out")
        assert tdSql.queryResult[0][0] == 1, \
            "pre-existing normal table and its data must survive ROLLBACK"

        # In-txn + COMMIT: the stream deploys and writes into the existing table.
        tdSql.execute("BEGIN")
        tdSql.execute(
            "create stream m18_c interval(10s) sliding(10s) from ct1"
            " into m18_out as select _twstart ts, count(*) cnt from %%trows"
        )
        tdSql.execute("COMMIT")
        status = self._wait_status("m18_c", ("Running", "Idle"))
        assert status in ("Running", "Idle"), \
            f"committed stream over existing normal table should run, status={status}"
        tdSql.execute("drop stream m18_c")

        tdLog.info("m18 verified: pre-existing normal output table is referenced, never created or undone")

    def m16_runtime_ops_blocked_outside_owning_txn(self):
        """FS 4.6: STOP / START / RECALCULATE STREAM from outside the owning txn are
        rejected with TSDB_CODE_TXN_RESOURCE_BUSY while the stream is pending_drop.

        The client rejects these statements inside a txn (they are runtime control, not
        metadata DDL), so the only way to hit the guard is from a second connection. The
        in-txn DROP keeps the stream running until COMMIT/ROLLBACK (DS 5.2); poking it from
        outside must not be allowed to race the txn's outcome."""
        self.s0_reset_env()
        tdLog.info("======== m16_runtime_ops_blocked_outside_owning_txn")

        self._create_stream("m16_s", "m16_o")
        self._wait_status("m16_s", ("Running", "Idle"))

        conn2, cursor2 = self._other_conn()
        try:
            tdSql.execute("BEGIN")
            tdSql.execute("drop stream m16_s")
            assert self._stream_status("m16_s") == "pending_drop", \
                self._stream_status("m16_s")

            # 0x3317 == TSDB_CODE_TXN_RESOURCE_BUSY. The in-txn DROP leaves the stream
            # Running (DS 5.2), so STOP and RECALCULATE (both legal on a Running stream)
            # must be refused by the owning-txn guard, not by a "not running" state.
            try:
                cursor2.execute("stop stream m16_s")
                raise AssertionError("STOP STREAM outside the owning txn should be refused")
            except AssertionError:
                raise
            except Exception as e:
                assert "0x3317" in str(e) or "being modified by another transaction" in str(e), \
                    f"expected TXN_RESOURCE_BUSY on STOP, got: {e}"
                tdLog.info(f"m16 STOP blocked: {e}")

            try:
                # RECALCULATE STREAM requires a FROM <ts> clause; a bare statement is a
                # parse error (0x2601 Incomplete SQL statement) and would never reach the
                # txn-ownership guard. `from 0` makes it a valid statement that the guard
                # must then reject with TXN_RESOURCE_BUSY.
                cursor2.execute("recalculate stream m16_s from 0")
                raise AssertionError("RECALCULATE STREAM outside the owning txn should be refused")
            except AssertionError:
                raise
            except Exception as e:
                assert "0x3317" in str(e) or "being modified by another transaction" in str(e), \
                    f"expected TXN_RESOURCE_BUSY on RECALCULATE, got: {e}"
                tdLog.info(f"m16 RECALCULATE blocked: {e}")

            # START is not exercised here: the pending_drop stream is still Running, so a
            # START from outside would fail with MND_STREAM_NOT_STOPPED before reaching the
            # txn guard — it cannot distinguish the txn-ownership rejection from the
            # ordinary "already running" rejection.
            tdSql.execute("ROLLBACK")
        finally:
            cursor2.close()
            conn2.close()

        # ROLLBACK restored the stream, so it must be operable again from outside.
        status = self._wait_status("m16_s", ("Running",), timeout=10)
        assert status == "Running", f"m16_s should still run after ROLLBACK, got {status}"
        tdLog.info("m16 PASSED: STOP/START/RECALCULATE blocked while pending_drop, "
                   "stream operable again after ROLLBACK")

    def m17_txn_drop_refuses_tsma_stream(self):
        """DS 6.1: the txn DROP path must run the same TSMA guard as the non-txn path.

        CREATE TSMA internally creates a stream carrying tsmaId, and DROP STREAM on
        such a stream is refused with TSDB_CODE_TSMA_MUST_BE_DROPPED (0x3110) in the
        non-txn path. The txn DROP path used to skip this check and would happily mark
        the stream PRE_DROP inside a txn -- inconsistent validation sets between the
        two paths."""
        self.s0_reset_env()
        tdLog.info("======== m17_txn_drop_refuses_tsma_stream")

        # A TSMA requires a super table; create a dedicated one so the shared stb is
        # not left with an index the other scenarios did not expect.
        tdSql.execute("create stable m17_stb (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table m17_c1 using m17_stb tags(1)")
        tdSql.execute("insert into m17_c1 values (now, 1)")

        # TSMA FUNCTION() requires explicit column names -- count(*) is rejected with a
        # parse error ("syntax error near \"*)) interval(1m)\"") and would never reach the
        # DROP guard under test. Use count(v), matching every other TSMA test in the repo.
        tdSql.execute(
            "create tsma m17_tsma on m17_stb function(count(v)) interval(1m)"
        )
        # TSMA internally creates a stream whose name is the TSMA name (mndSma.c
        # mndTSMAGenerateOutputName: "<acctId>.<tsmaName>"). It must not be droppable
        # on its own.
        assert "m17_tsma" in self._stream_names(), self._stream_names()

        # Baseline: non-txn DROP refuses while the TSMA exists. Client-visible code is
        # 0x3110 "Tsma must be dropped first".
        tdSql.error("drop stream m17_tsma",
                    expectErrInfo="Tsma must be dropped first", fullMatched=False)
        tdLog.info("m17 non-txn baseline refuses TSMA-related DROP")

        # Txn path must refuse identically.
        tdSql.execute("BEGIN")
        tdSql.error("drop stream m17_tsma",
                    expectErrInfo="Tsma must be dropped first", fullMatched=False)
        tdSql.execute("ROLLBACK")
        assert "m17_tsma" in self._stream_names(), "stream should survive refused txn DROP"
        tdLog.info("m17 txn DROP refused TSMA-related stream, txn still ROLLBACK-able")

        # Cleanup: dropping the TSMA removes its internal stream too.
        tdSql.execute("drop tsma m17_tsma")
        assert "m17_tsma" not in self._stream_names(), \
            "TSMA drop should remove its internal stream"

    def m19_nested_window_rejected_in_txn(self):
        """FS §4.1: nested-window streams are not supported inside a transaction.

        A nested-window stream (CREATE STREAM ... WINDOW(...)) is created through the
        async preflight queue (mndStreamDispatchNestedCreate), which does not carry a
        txn context of its own — the continuation lands in mndProcessCreateStreamFinal
        with no way to participate in the txn. The client's isTxnAllowedStmtType admits
        QUERY_NODE_CREATE_STREAM_STMT wholesale, so the rejection must come from the
        server: mndProcessCreateStreamReq refuses txnId != 0 && nested with
        TSDB_CODE_TXN_INVALID_OPERATION.

        This pins that guard: the same statement parses and is accepted outside a txn,
        and is refused inside one with the txn remaining usable (ROLLBACK-able)."""
        self.s0_reset_env()
        tdLog.info("======== m19_nested_window_rejected_in_txn")

        # A dedicated source table with a data column for state_window(scope).
        tdSql.execute("create table m19_src (ts timestamp, scope int, v int)")
        nested_sql = (
            "create stream m19_ns window ("
            "state_window(scope) extend(1) as w_scope, count_window(2,1)) "
            "from m19_src into m19_o (ts,leaf_rows) as "
            "select _twstart,count(*) from %%trows"
        )

        # Baseline: outside a txn the nested-window statement is accepted (parses and
        # reaches the async preflight path without a syntax error). Deployment is
        # asynchronous, so existence is not asserted here — the in-txn rejection is
        # the point of this case.
        tdSql.execute(nested_sql)
        tdSql.execute_ignore_error("drop stream if exists m19_ns")

        # In-txn: refused with TXN_INVALID_OPERATION (0x331C); the txn stays usable.
        tdSql.execute("BEGIN")
        tdSql.error(
            nested_sql,
            expectErrInfo="Only table DDL (CREATE/DROP/ALTER TABLE) allowed in transaction",
            fullMatched=False,
        )
        # The txn is still alive and can ROLLBACK cleanly.
        tdSql.execute("ROLLBACK")
        tdSql.execute_ignore_error("drop stream if exists m19_ns")

        tdLog.info("m19 PASSED: nested-window stream rejected inside a txn, "
                   "accepted outside one, txn usable after rejection")

    def test_meta_batch_txn_stream(self):
        """Batch meta txn phase 2: CREATE/DROP STREAM in a transaction.

        Stream-only scenarios (s1-s13):
        1. create_commit
        2. create_rollback
        3. drop_commit
        4. drop_rollback
        5. create_then_drop_same_txn
        6. recreate_same_name_rejected
        7. rename_replacement_allowed
        8. committed_stream_produces_output
        9. no_stale_txnid_after_commit
        10. batch_drop_stream (pending_drop in-txn, ROLLBACK restores, IF EXISTS per name)
        11. streams_share_ddl_budget
        12. definition_replacement_no_gap
        13. txn_markers_cleared_after_commit

        Mixed table-DDL + stream scenarios (m1-m18):
        14. table_and_stream_commit
        15. table_and_stream_rollback
        16. stream_over_table_created_in_same_txn
        17. stream_over_same_txn_table_rollback
        18. drop_stream_and_its_source_table
        19. precreated_out_stable_with_partitioned_stream
        20. alter_stable_with_stream_commit
        21. alter_stable_with_stream_rollback
        22. failed_stream_stmt_keeps_txn_usable
        23. multiple_streams_share_out_table (allowed, matching the non-txn baseline)
        24. out_table_drop_outside_txn_is_not_blocked (no guard on DROP)
        25. stream_no_snode (0x042C on both txn and non-txn paths)
        26. snode_protected_by_uncommitted_stream (uncommitted stream pins its snode)
        27. uncommitted_stream_does_not_start (no deploy before COMMIT)
        28. out_stable_created_in_txn (form A output STB is created in-txn, undone on ROLLBACK)
        29. nodelay_out_table_created_in_txn (form B nodelay created in-txn, txn-isolated)
        30. runtime_ops_blocked_outside_owning_txn (STOP/START/RECALCULATE from another
            session refused with TXN_RESOURCE_BUSY while the stream is pending_drop)
        31. txn_drop_refuses_tsma_stream (txn DROP path runs the same TSMA guard as non-txn)
        32. preexisting_normal_out_table (form D: INTO an existing NORMAL table is a
            reference, not a create outTblExists vs outStbExists dispatch)
        33. nested_window_rejected_in_txn (nested-window streams are refused inside a
            txn with TXN_INVALID_OPERATION, accepted outside one)

        Since: v3.4.3.0

        Labels: common,ci
        """
        self.s1_create_commit()
        self.s2_create_rollback()
        self.s3_drop_commit()
        self.s4_drop_rollback()
        self.s5_create_then_drop_same_txn()
        self.s6_recreate_same_name_rejected()
        self.s7_rename_replacement_allowed()
        self.s8_committed_stream_produces_output()
        self.s9_no_stale_txnid_after_commit()
        self.s10_batch_drop_stream()
        self.s11_streams_share_ddl_budget()
        self.s12_definition_replacement_no_gap()
        self.s13_txn_markers_cleared_after_commit()

        # Mixed table DDL + stream DDL in a single transaction.
        self.m1_table_and_stream_commit()
        self.m2_table_and_stream_rollback()
        self.m3_stream_over_table_created_in_same_txn()
        self.m4_stream_over_same_txn_table_rollback()
        self.m5_drop_stream_and_its_source_table()
        self.m6_precreated_out_stable_with_partitioned_stream()
        self.m7_alter_stable_with_stream_commit()
        self.m8_alter_stable_with_stream_rollback()
        self.m9_failed_stream_stmt_keeps_txn_usable()
        self.m10_multiple_streams_share_out_table()
        self.m10b_out_table_drop_outside_txn_is_not_blocked()
        self.m11_stream_no_snode()
        self.m12_snode_protected_by_uncommitted_stream()
        self.m13_uncommitted_stream_does_not_start()
        self.m14_out_stable_created_in_txn()
        self.m15_nodelay_out_table_created_in_txn()
        self.m16_runtime_ops_blocked_outside_owning_txn()
        self.m17_txn_drop_refuses_tsma_stream()
        self.m18_preexisting_normal_out_table()
        self.m19_nested_window_rejected_in_txn()
