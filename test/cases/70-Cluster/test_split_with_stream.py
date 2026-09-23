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
"""Test: interaction between ``SPLIT VGROUP`` and stream computation.

Observed product behavior (v3.4)
--------------------------------
When a database is being used by a stream, ``SPLIT VGROUP`` on that
database's vgroup is **rejected** with error ``[0x700e]: Db used by
stream``. In other words, splitting is NOT allowed while a stream is
attached to the database.

Scenario A -- stream exists first, then split (``test_split_with_stream``)
-------------------------------------------------------------------------
1. create a snode and a source super table on a single-vgroup database
2. create a stream that aggregates the source table into an output stable
3. start a background writer that keeps feeding rows to the source table
4. attempt ``split vgroup`` -> assert it is rejected with "Db used by stream"
5. drop the stream, then attempt the split again -> assert it now succeeds

This documents that stream compute BLOCKS split, and that split works once
the stream is removed.

Scenario B -- split first, then stream ops (``test_stream_ops_during_split``)
----------------------------------------------------------------------------
The reverse ordering: fire ``split vgroup`` and, *while the split transaction
is still in flight*, try stream operations on the same database:

1. prepare a fresh single-vgroup database with data (no stream attached)
2. trigger ``split vgroup`` without waiting for it to finish
3. while the split transaction is still pending, attempt to
   ``create stream`` on that database, and also attempt ``drop stream``
4. record whether those stream operations succeed or are rejected
5. wait for the split to complete, then verify a stream can be created
   normally once the split transaction is gone

This documents whether stream DDL is allowed during an ongoing split.
"""

import time
import threading

from new_test_framework.utils import tdLog, tdSql, tdStream
from split_concurrent_util import (
    new_connection,
    get_vgroups,
    try_split_first_vgroup,
    split_first_vgroup,
    trigger_split_first_vgroup,
    wait_transactions_clear,
    transactions_pending,
)


class TestSplitWithStream:
    dbname = "split_stream"
    stream_name = "s_split"
    child_num = 6

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        # a snode is required to run streams in v3.4
        tdStream.createSnode()

        # single vgroup db so the split target is deterministic
        tdSql.execute(f"drop database if exists {cls.dbname}")
        tdSql.execute(f"create database {cls.dbname} vgroups 1 replica 1")
        tdSql.execute(f"use {cls.dbname}")

        # source super table + child tables
        tdSql.execute(
            f"create stable {cls.dbname}.stb "
            f"(ts timestamp, c1 int) tags (t1 int)"
        )
        base = 1700000000000
        for i in range(cls.child_num):
            tdSql.execute(
                f"create table {cls.dbname}.c{i} using {cls.dbname}.stb tags({i})"
            )
            # seed some rows so the stream has an initial window to compute
            values = " ".join(
                f"({base + r * 1000}, {r})" for r in range(200)
            )
            tdSql.execute(f"insert into {cls.dbname}.c{i} values {values}")

        # output stable for the stream result
        tdSql.execute(
            f"create stable {cls.dbname}.out_stb "
            f"(ts timestamp, cnt bigint, s bigint) tags (t1 int)"
        )
        # stream: 1s interval aggregation, partition by source table
        tdSql.execute(
            f"create stream {cls.dbname}.{cls.stream_name} "
            f"interval(1s) sliding(1s) from {cls.dbname}.stb "
            f"partition by tbname, t1 "
            f"into {cls.dbname}.out_stb tags (t1 int as t1) "
            f"as select _twstart as ts, count(c1) as cnt, sum(c1) as s from %%trows"
        )
        # wait for the stream trigger task to reach Running
        tdStream.checkStreamStatus(cls.stream_name)
        tdLog.info(f"[stream] stream ready, vgroups={get_vgroups(cls.dbname)}")

    # ------------------------------------------------------------------
    # background writer
    # ------------------------------------------------------------------
    def _writer_worker(self, stop_evt):
        """Keep inserting fresh rows so the stream keeps producing output."""
        conn = new_connection()
        cur = conn.cursor()
        cur.execute(f"use {self.dbname}")
        ts = 1800000000000
        n = 0
        while not stop_evt.is_set():
            # Advance the counter BEFORE the insert so every attempt gets a
            # unique timestamp. If the insert fails, the next round retries a
            # different row instead of hammering the same (ts, n) forever and
            # stalling the stream output.
            n += 1
            try:
                tb = f"c{n % self.child_num}"
                cur.execute(
                    f"insert into {self.dbname}.{tb} values ({ts + n * 1000}, {n % 100})"
                )
            except Exception as e:  # noqa: BLE001
                tdLog.info(f"[stream-writer] insert failed: {e}")
            time.sleep(0.05)
        cur.close()
        conn.close()
        tdLog.info(f"[stream-writer] stopped after {n} attempts")

    def _stream_output_rows(self):
        """Return current number of rows in the stream output stable."""
        tdSql.query(f"select count(*) from {self.dbname}.out_stb")
        return tdSql.getData(0, 0)

    def test_split_with_stream(self):
        """SPLIT VGROUP 期间运行流计算

        验证在 split vgroup 事务进行过程中，流计算是否能够继续运行并产生
        正确的输出结果。

        Since: v3.4.0.0

        Labels: cluster,split,stream,functional

        Jira: None

        History:
            - 2026-06-13 Created
        """
        stop_evt = threading.Event()
        writer = threading.Thread(
            target=self._writer_worker, args=(stop_evt,), daemon=True
        )

        rows_before = self._stream_output_rows()
        tdLog.info(f"[stream] output rows before split attempt = {rows_before}")

        tdLog.info("[stream] start background writer")
        writer.start()
        time.sleep(2)

        # ---- step 1: split MUST be rejected while a stream uses the db ----
        vgid, ok, err = try_split_first_vgroup(self.dbname)
        tdLog.info(f"[stream] split attempt on vgroup {vgid}: ok={ok}, err={err}")
        stop_evt.set()
        writer.join(timeout=30)

        assert not ok, (
            "split unexpectedly succeeded while a stream was using the db; "
            "expected it to be rejected with 'Db used by stream'"
        )
        assert "Db used by stream" in err or "0x700e" in err, (
            f"split rejected but with an unexpected error: {err}"
        )
        # vgroup count must NOT have changed since the split was blocked
        blocked_vgroups = get_vgroups(self.dbname)
        assert len(blocked_vgroups) == 1, (
            f"vgroup count changed despite blocked split: {blocked_vgroups}"
        )
        tdLog.info("[stream] confirmed: split is blocked while stream uses the db")

        # ---- step 2: drop the stream, then the split should succeed ----
        tdLog.info("[stream] dropping stream and retrying split")
        tdSql.execute(f"drop stream {self.dbname}.{self.stream_name}")
        # drop stream returns before the mnode has removed the stream object
        # from SDB (the drop is applied via a stream transaction), while the
        # split check reads the stream table directly; a stale entry would
        # still raise 0x700E/0x700F. Poll instead of a fixed sleep so the
        # retried split is deterministic (same rationale as the
        # _wait_stream_gone() helper in test_split_vgroup_force.py).
        self._wait_stream_gone(self.stream_name)
        vgid2, elapsed = split_first_vgroup(self.dbname)
        tdLog.info(f"[stream] split of vgroup {vgid2} took {elapsed:.1f}s after drop")

        final_vgroups = get_vgroups(self.dbname)
        assert len(final_vgroups) >= 2, (
            f"split did not increase vgroup count after dropping stream, "
            f"got {final_vgroups}"
        )
        tdLog.info("[stream] test_split_with_stream PASSED")

    def _wait_stream_gone(self, stream_name, timeout=60):
        """Block until ``stream_name`` disappears from ``show streams``.

        ``drop stream`` returns before the mnode removes the stream object
        from its SDB (the drop is applied via a stream transaction), but the
        split check reads the stream table directly, so a stale entry would
        still raise 0x700E/0x700F. Poll with a timeout instead of a fixed
        sleep; same rationale as test_split_vgroup_force.py's helper.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            tdSql.query("show streams")
            names = [row[0] for row in (tdSql.queryResult or [])]
            if stream_name not in names:
                tdLog.info(f"[stream-wait] stream {stream_name} is gone")
                return
            tdLog.info(f"[stream-wait] stream {stream_name} still present: {names}")
            time.sleep(1)
        tdLog.exit(f"[stream-wait] stream {stream_name} still exists after {timeout}s")

    # ==================================================================
    # Scenario B: split first, then stream operations during the split
    # ==================================================================
    def _prepare_split_first_db(self, db):
        """Create a fresh single-vgroup db with data and NO stream attached.

        Scenario B must start from a database that is *not* used by any
        stream, otherwise the split would be rejected up-front by the
        behavior proven in scenario A.
        """
        tdLog.info(f"[stream-b] preparing db {db} (single vgroup, no stream)")
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 1")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create stable {db}.stb (ts timestamp, c1 int) tags (t1 int)"
        )
        tdSql.execute(
            f"create stable {db}.out_stb "
            f"(ts timestamp, cnt bigint, s bigint) tags (t1 int)"
        )

        # generate enough data so the split has real vnode work to do and
        # therefore stays in flight long enough for us to race with it
        base = 1700000000000
        for i in range(self.child_num):
            tdSql.execute(f"create table {db}.c{i} using {db}.stb tags({i})")
            for chunk in range(5):
                values = " ".join(
                    f"({base + (chunk * 1000 + r) * 1000}, {r})"
                    for r in range(1000)
                )
                tdSql.execute(f"insert into {db}.c{i} values {values}")
        tdLog.info(f"[stream-b] db {db} ready, vgroups={get_vgroups(db)}")

    def _try_stream_stmt(self, sql, label):
        """Run one stream DDL on its own connection, never raising.

        Returns ``(ok, err_text)``. A dedicated connection is used so the
        framework's retry wrapper does not mask a rejection.
        """
        conn = new_connection()
        cur = conn.cursor()
        try:
            cur.execute(sql)
            tdLog.info(f"[stream-b] {label}: ACCEPTED")
            return True, ""
        except Exception as e:  # noqa: BLE001 - raw message goes to the report
            tdLog.info(f"[stream-b] {label}: REJECTED -> {e}")
            return False, str(e)
        finally:
            cur.close()
            conn.close()

    def test_stream_ops_during_split(self):
        """SPLIT VGROUP 执行过程中执行 stream 操作

        先在没有 stream 的库上触发 split vgroup，在 split 事务仍在进行的
        窗口内，尝试在同一个库上执行 stream 操作（create stream / drop
        stream），记录这些操作是否被允许；split 结束后再验证 stream 能否
        正常创建。

        Since: v3.4.0.0

        Labels: cluster,split,stream,functional

        Jira: None

        History:
            - 2026-08-10 Created
        """
        db = "split_first_stream"
        stream_b = "s_during_split"
        self._prepare_split_first_db(db)

        # ---- step 1: fire the split WITHOUT waiting for it to finish ----
        vgid = trigger_split_first_vgroup(db)

        # The split's data movement is async: `split vgroup` returns as soon
        # as the mnode transaction is created. Wait (bounded) until the
        # transaction is actually visible in `show transactions`, so the race
        # loop below is guaranteed to overlap it. If it never shows up, the
        # split already finished before we could observe it -- fail
        # deterministically with actionable guidance instead of flaking on
        # `overlapped == 0` below.
        pending = 0
        appear_deadline = time.time() + 10
        while time.time() < appear_deadline:
            pending = transactions_pending()
            if pending > 0:
                break
            time.sleep(0.05)
        tdLog.info(
            f"[stream-b] split of vgroup {vgid} submitted, "
            f"pending transactions={pending}"
        )
        assert pending > 0, (
            "split transaction never became visible within 10s; the split "
            "finished before the race loop could start. Increase the seed "
            "data in _prepare_split_first_db so the split stays in flight "
            "long enough to be overlapped by stream operations"
        )

        # ---- step 2: race stream DDL against the in-flight split ----
        # Retry in a tight loop while the split transaction is still pending
        # so we are certain at least one attempt truly overlapped the split.
        create_sql = (
            f"create stream {db}.{stream_b} "
            f"interval(1s) sliding(1s) from {db}.stb "
            f"partition by tbname, t1 "
            f"into {db}.out_stb tags (t1 int as t1) "
            f"as select _twstart as ts, count(c1) as cnt, sum(c1) as s from %%trows"
        )

        # Template with a per-round stream name. Each round uses its OWN name
        # ({stream_b}_<round>): if a round's create succeeds but the follow-up
        # drop fails, the stream stays alive, and reusing the same name on the
        # next round would then fail with "stream already exists" - a failure
        # that is NOT caused by the split transaction and would pollute the
        # create_bad counter in the summary below.
        create_sql_tpl = (
            f"create stream {db}.{stream_b}_%d "
            f"interval(1s) sliding(1s) from {db}.stb "
            f"partition by tbname, t1 "
            f"into {db}.out_stb tags (t1 int as t1) "
            f"as select _twstart as ts, count(c1) as cnt, sum(c1) as s "
            f"from %%trows"
        )

        attempts = []          # (round, op, ok, err) observed during the split
        created_streams = []   # streams created mid-split, for later cleanup
        overlapped = 0         # how many attempts were made while split pending
        rnd = 0
        deadline = time.time() + 120
        # Upper bound on how many mid-split streams the race loop is willing
        # to leave alive at the same time. The loop deliberately probes the
        # branch where a mid-split create succeeds but the follow-up drop
        # does not; if the product under test behaves exactly that way, one
        # leftover stream would be stacked every 0.3s until the split ends or
        # the 120s deadline hits -- up to hundreds of streams, each with a
        # running trigger task on the snode, hammering the mnode. Stop racing
        # as soon as the behavior is clearly observed (this many samples).
        max_leftover_streams = 10
        live_leftovers = 0     # created streams whose drop did NOT succeed
        while transactions_pending() > 0 and time.time() < deadline:
            if live_leftovers >= max_leftover_streams:
                tdLog.info(
                    f"[stream-b] {live_leftovers} streams left alive by "
                    "mid-split create/drop races; stopping loop to avoid "
                    "stacking up leftover streams"
                )
                break
            rnd += 1
            overlapped += 1
            stream_name = f"{stream_b}_{rnd}"
            ok, err = self._try_stream_stmt(
                create_sql_tpl % rnd,
                f"round {rnd} create stream during split",
            )
            attempts.append((rnd, "create_stream", ok, err))

            if ok:
                created_streams.append(stream_name)
                # the stream got created mid-split; also probe drop stream
                ok2, err2 = self._try_stream_stmt(
                    f"drop stream {db}.{stream_name}",
                    f"round {rnd} drop stream during split",
                )
                attempts.append((rnd, "drop_stream", ok2, err2))
                if ok2:
                    # a full create -> drop round-trip was already observed
                    # while the split was pending, so the behavior is captured;
                    # stop racing to avoid stacking up leftover streams
                    tdLog.info(
                        f"[stream-b] create+drop succeeded in round {rnd} "
                        "while split pending; stopping race loop"
                    )
                    break
                # create succeeded but drop was rejected: the stream stays
                # alive, so count it towards the leftover bound above.
                live_leftovers += 1
            time.sleep(0.3)

        tdLog.info(
            f"[stream-b] attempts made while split was pending: {overlapped}"
        )
        for a in attempts:
            tdLog.info(
                f"[stream-b] result: round={a[0]} op={a[1]} ok={a[2]} err={a[3]}"
            )

        assert overlapped > 0, (
            "split transaction finished before any stream op could be "
            "attempted; generate more data so the split takes longer"
        )

        # ---- step 3: let the split finish ----
        elapsed = wait_transactions_clear()
        after = get_vgroups(db)
        tdLog.info(
            f"[stream-b] split done in {elapsed:.1f}s, vgroups now {after}"
        )
        assert len(after) >= 2, (
            f"split did not increase vgroup count, got {after}"
        )

        # ---- step 4: after the split, stream creation must work ----
        # clean up any streams a mid-split create actually left behind
        for sname in created_streams:
            try:
                tdSql.execute(f"drop stream if exists {db}.{sname}")
            except Exception as e:  # noqa: BLE001
                tdLog.info(f"[stream-b] pre-clean drop stream {sname} ignored: {e}")

        ok_after, err_after = self._try_stream_stmt(
            create_sql, "create stream after split completed"
        )
        assert ok_after, (
            f"stream could not be created even after the split finished: {err_after}"
        )
        tdStream.checkStreamStatus(stream_b)
        tdLog.info("[stream-b] stream created and running after split")

        # summarize for the test report
        create_ok = sum(1 for a in attempts if a[1] == "create_stream" and a[2])
        create_bad = sum(
            1 for a in attempts if a[1] == "create_stream" and not a[2]
        )
        tdLog.info(
            f"[stream-b] SUMMARY create stream during split: "
            f"ok={create_ok}, rejected={create_bad}"
        )
        tdLog.info("[stream-b] test_stream_ops_during_split PASSED")

    def teardown_class(cls):
        """Best-effort cleanup so a failed run does not poison the next one.

        Streams live in the mnode, not in the database. A leftover stream
        keeps raising cluster-wide 0x700E/0x700F for every later split test
        (see the docstring and teardown of test_split_vgroup_force.py, and
        mstChkSetDbInUse() which scans ALL streams in the cluster).
        Drop every stream this class may have created, then drop the dbs.
        """
        # Scenario B's race loop names streams s_during_split_<round>; if the
        # test aborted early we do not know how many were created, so scan
        # `show streams` and drop anything with that prefix.
        try:
            tdSql.query("show streams")
            names = [row[0] for row in (tdSql.queryResult or [])]
        except Exception as e:  # noqa: BLE001 - teardown must not mask failures
            tdLog.info(f"[stream-teardown] show streams failed: {e}")
            names = []
        for sname in names:
            if sname.startswith("s_during_split"):
                try:
                    tdSql.execute(
                        f"drop stream if exists split_first_stream.{sname}"
                    )
                except Exception as e:  # noqa: BLE001
                    tdLog.info(f"[stream-teardown] drop stream {sname} failed: {e}")

        # Fixed names: scenario A (s_split) and scenario B step 4 (s_during_split).
        # The s_during_split prefix match above already covers the latter, so
        # this second pass is defensive only (IF EXISTS makes it a no-op).
        for stream in (
            f"{cls.dbname}.{cls.stream_name}",        # split_stream.s_split
            "split_first_stream.s_during_split",      # scenario B step 4
        ):
            try:
                tdSql.execute(f"drop stream if exists {stream}")
            except Exception as e:  # noqa: BLE001 - teardown must not mask failures
                tdLog.info(f"[stream-teardown] drop stream {stream} failed: {e}")

        for db in (cls.dbname, "split_first_stream"):
            try:
                tdSql.execute(f"drop database if exists {db} force")
            except Exception as e:  # noqa: BLE001 - teardown must not mask failures
                tdLog.info(f"[stream-teardown] drop database {db} failed: {e}")
        tdLog.success(f"{__file__} successfully executed")
