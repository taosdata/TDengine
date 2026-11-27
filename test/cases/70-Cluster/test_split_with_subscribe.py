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
"""Test: consume a TMQ subscription while ``SPLIT VGROUP`` is in flight.

Scenario
--------
1. generate data on a single-vgroup database
2. create a topic over the super table
3. start a background consumer that keeps polling the topic
4. keep writing new rows so the consumer always has something to fetch
5. trigger ``split vgroup`` on the main thread
6. record whether the consumer kept polling / committing successfully
   while the split transaction was running
"""

import time
import threading

from taos.tmq import Consumer

from new_test_framework.utils import tdLog, tdSql, etool
from split_concurrent_util import new_connection, get_vgroups, split_first_vgroup


class TestSplitWithSubscribe:
    dbname = "split_tmq"
    topic = "topic_split_tmq"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        # single vgroup db, filled with benchmark data (stable "meters")
        tdSql.execute(f"drop database if exists {cls.dbname}")
        tdSql.execute(f"create database {cls.dbname} vgroups 1 replica 1 wal_retention_period 3600")
        etool.benchMark(command=f"-d {cls.dbname} -t 10 -n 5000 -v 1 -y")
        # topic over the super table created by taosBenchmark ("meters")
        tdSql.execute(f"drop topic if exists {cls.topic}")
        tdSql.execute(
            f"create topic {cls.topic} as select ts, current, voltage from {cls.dbname}.meters"
        )
        tdLog.info(f"[tmq] topic created, vgroups={get_vgroups(cls.dbname)}")

    # ------------------------------------------------------------------
    # background workers
    # ------------------------------------------------------------------
    def _writer_worker(self, stop_evt, stats, stats_lock):
        """Keep appending rows so the consumer always has new data to poll.

        Records each insert attempt outcome in ``stats`` (a dict with "ok" /
        "fail" counts) so the test can tell "subscription consuming new data"
        apart from "writes silently failed and the consumer was starved".

        ``stats`` is shared with the main thread, which reads it after
        ``join(timeout=...)``. The timeout does not guarantee the worker
        actually exited (a still-alive worker keeps writing), so every update
        takes ``stats_lock`` to avoid a read-write race on the dict.
        """
        conn = new_connection()
        cur = conn.cursor()
        cur.execute(f"use {self.dbname}")
        ts = 1800000000000
        n = 0
        while not stop_evt.is_set():
            # Advance the counter BEFORE the insert so every attempt gets a
            # unique timestamp. If the insert fails, the next round retries a
            # different row instead of hammering the same (ts, n) forever and
            # stalling the data the consumer is supposed to receive.
            n += 1
            try:
                cur.execute(
                    f"insert into {self.dbname}.d0 values "
                    f"({ts + n * 1000}, {n % 100}, {200 + n % 50}, 0.5)"
                )
                with stats_lock:
                    stats["ok"] += 1
            except Exception as e:  # noqa: BLE001
                with stats_lock:
                    stats["fail"] += 1
                tdLog.info(f"[tmq-writer] insert failed: {e}")
            time.sleep(0.05)
        cur.close()
        conn.close()
        with stats_lock:
            ok_n, fail_n = stats["ok"], stats["fail"]
        tdLog.info(
            f"[tmq-writer] stopped, attempts={n}, ok={ok_n}, fail={fail_n}"
        )

    def _consumer_worker(self, results, stop_evt, stats_lock):
        """Poll the topic until stopped, recording each poll outcome.

        ``results`` is shared with the main thread, which reads it after
        ``join(timeout=...)``; the timeout does not guarantee the worker
        exited, so every append takes ``stats_lock`` (same reason as the
        writer's ``stats`` updates).

        Every attempt is recorded as ``(round, ok, rows, err, t_start, t_end)``
        where ``t_start`` / ``t_end`` are ``time.time()`` values taken
        immediately before and after the poll. The main thread keeps the
        polls that OVERLAP the split window -- i.e. any part of the poll ran
        while the split transaction was in flight. The consumer starts 2s
        before the split (and keeps polling 2s after), and with 50k rows of
        backlog plus auto.offset.reset=earliest the polls that ran entirely
        outside the window would succeed anyway and mask a real regression
        (same rationale as test_split_with_vtable_query.py).

        NOTE: the predicate is OVERLAP, not "fully inside the window".
        poll() blocks up to its timeout while the split holds the vnode
        lock, so a poll started just before the split may return just after
        it -- that poll is exactly the one that observed the split, and
        requiring it to be fully contained would discard it and make the
        test flakily report "no poll inside the split window". To further
        shrink the odds of a window with no poll at all, the poll timeout
        and fetch.max.wait.ms are kept small (200ms) so polls are issued
        several times per second instead of once per second.
        """
        consumer = Consumer(
            {
                "group.id": "cgrp_split",
                "client.id": "client_split",
                "td.connect.user": "root",
                "td.connect.pass": "taosdata",
                "enable.auto.commit": "true",
                "auto.commit.interval.ms": "1000",
                "auto.offset.reset": "earliest",
                "td.connect.ip": "localhost",
                "td.connect.port": "6030",
                # keep the fetch wait small so each poll returns quickly and
                # polls are issued several times per second -- with a 1s
                # timeout a poll that spans the (typically 1-3s) split window
                # would be counted OUT of the window and the test could
                # flakily fail with "no poll inside the split window".
                "fetch.max.wait.ms": "200",
            }
        )
        consumer.subscribe([self.topic])
        rnd = 0
        total_rows = 0
        while not stop_evt.is_set():
            rnd += 1
            t0 = time.time()
            try:
                # 200ms poll timeout: keeps poll frequency high so at least
                # one poll is statistically guaranteed to overlap the split
                # window even when the split is short (see module docstring).
                msg = consumer.poll(0.2)
                t1 = time.time()
                if msg is None:
                    with stats_lock:
                        results.append((rnd, True, 0, "empty", t0, t1))
                    continue
                err = msg.error()
                if err is not None:
                    with stats_lock:
                        results.append((rnd, False, 0, str(err), t0, t1))
                    tdLog.info(f"[tmq-consumer] FAIL #{rnd} {err}")
                    continue
                rows = 0
                for block in msg:
                    rows += len(block.fetchall())
                total_rows += rows
                with stats_lock:
                    results.append((rnd, True, rows, "", t0, t1))
            except Exception as e:  # noqa: BLE001
                with stats_lock:
                    results.append((rnd, False, 0, str(e), t0, time.time()))
                tdLog.info(f"[tmq-consumer] EXC  #{rnd} {e}")
        consumer.close()
        tdLog.info(f"[tmq-consumer] stopped, total_rows={total_rows}")

    def test_split_with_subscribe(self):
        """SPLIT VGROUP 期间进行数据订阅消费

        验证在 split vgroup 事务进行过程中，TMQ 消费者是否能够持续正常地
        poll 到数据。

        Since: v3.4.0.0

        Labels: cluster,split,tmq,subscribe,functional

        Jira: None

        History:
            - 2026-06-13 Created
        """
        results = []
        stats = {"ok": 0, "fail": 0}
        # Guards the shared stats dict and results list. Both workers write
        # these while the main thread reads them after join(timeout=30) --
        # the timeout does not guarantee the workers exited (see below), so
        # every write takes the lock and the main thread takes a consistent
        # snapshot before analysing.
        stats_lock = threading.Lock()
        stop_evt = threading.Event()
        writer = threading.Thread(
            target=self._writer_worker, args=(stop_evt, stats, stats_lock),
            daemon=True,
        )
        consumer = threading.Thread(
            target=self._consumer_worker, args=(results, stop_evt, stats_lock),
            daemon=True,
        )

        tdLog.info("[tmq] start writer + consumer workers")
        writer.start()
        consumer.start()
        time.sleep(2)

        # ------------------------------------------------------------------
        # main thread performs the split and blocks until it finishes.
        # split_start / split_end bracket the split window; only poll attempts
        # that fall completely inside this window are meaningful for the
        # "can it consume during split?" question.
        # ------------------------------------------------------------------
        split_start = time.time()
        vgid, elapsed = split_first_vgroup(self.dbname)
        split_end = time.time()
        tdLog.info(
            f"[tmq] split of vgroup {vgid} took {elapsed:.1f}s "
            f"(window {split_start:.3f} .. {split_end:.3f})"
        )

        # let the consumer poll a bit more after the split completes
        time.sleep(2)
        stop_evt.set()
        writer.join(timeout=30)
        consumer.join(timeout=30)
        # join() with a timeout does not guarantee the threads exited: a
        # still-alive worker keeps holding its connection / consumer object
        # and may keep appending to results/stats (data used by the asserts
        # below) until the pytest process exits. daemon threads cannot be
        # killed, so record the situation in the log and proceed. All further
        # analysis works on the snapshot below, so a still-running worker
        # cannot race with the reads (each of its writes holds stats_lock).
        if writer.is_alive():
            tdLog.info(
                "[tmq] writer thread still alive after join timeout; "
                "it keeps running until the pytest process exits"
            )
        if consumer.is_alive():
            tdLog.info(
                "[tmq] consumer thread still alive after join timeout; "
                "it keeps running until the pytest process exits"
            )

        # Consistent snapshot: afterwards the worker threads cannot affect
        # the values the assertions and the report below are based on.
        with stats_lock:
            stats_snapshot = dict(stats)
            results_snapshot = list(results)

        # Keep the polls that OVERLAP the split window: any poll that ran
        # (at least partly) while the split transaction was in flight is
        # meaningful. Polls that ran entirely before/after the split tell us
        # nothing about concurrent behavior -- and with 50k rows of backlog
        # data plus auto.offset.reset=earliest they would likely succeed
        # anyway. We deliberately do NOT require the poll to be fully
        # contained: while the split holds the vnode lock, poll() blocks up
        # to its timeout, so a poll that started just before the split and
        # returned just after it is the very one that observed the split.
        in_window = [
            r for r in results_snapshot if r[5] >= split_start and r[4] <= split_end
        ]
        outside = len(results_snapshot) - len(in_window)

        total = len(in_window)
        ok = sum(1 for r in in_window if r[1])
        failed = total - ok
        got_rows = sum(r[2] for r in in_window if r[1])
        tdLog.info(
            f"[tmq] poll attempts overlapping split window: total={total}, ok={ok}, "
            f"failed={failed}, rows={got_rows}, writer_ok={stats_snapshot['ok']}, "
            f"writer_fail={stats_snapshot['fail']} "
            f"(discarded {outside} poll(s) outside the window)"
        )
        # dump everything, flagging whether each poll overlapped the window
        for r in results_snapshot:
            scope = "IN " if (r[5] >= split_start and r[4] <= split_end) else "OUT"
            tdLog.info(
                f"[tmq] result: scope={scope} round={r[0]} ok={r[1]} rows={r[2]}"
            )

        # We must have actually polled *during* the split, otherwise the test
        # proves nothing and should fail loudly instead of passing on polls
        # made outside the window.
        assert total > 0, (
            "no consumer poll overlapped the split window; the split likely "
            "finished too fast for any poll() to run during it"
        )
        assert ok > 0, (
            "every in-window consumer poll failed during the split "
            f"({total} attempts)"
        )
        assert got_rows > 0, (
            "consumer never actually received any data rows inside the split "
            "window (only empty polls succeeded)"
        )
        assert stats_snapshot["ok"] > 0, (
            "writer never inserted successfully, so the consumer had no new "
            "data to consume during split (writes silently failed)"
        )

        final_vgroups = get_vgroups(self.dbname)
        assert len(final_vgroups) >= 2, (
            f"split did not increase vgroup count, got {final_vgroups}"
        )
        tdLog.info("[tmq] test_split_with_subscribe PASSED")

    def teardown_class(cls):
        """Best-effort cleanup so a failed run does not poison the next one.

        The consumer group cgrp_split uses enable.auto.commit, so its
        committed offsets are persisted in the vnode's tq metadata (keyed by
        groupId+topicName). Dropping the topic and then the database removes
        them along with the vnode (vnodeDestroy deletes the whole data
        directory), so a later run always starts from earliest instead of
        from a stale committed position. setup_class already rebuilds the db,
        but explicit teardown keeps the environment clean even when a run
        fails midway.
        """
        try:
            tdSql.execute(f"drop topic if exists {cls.topic}")
        except Exception as e:  # noqa: BLE001 - teardown must not mask failures
            tdLog.info(f"[tmq-teardown] drop topic {cls.topic} failed: {e}")
        try:
            tdSql.execute(f"drop database if exists {cls.dbname} force")
        except Exception as e:  # noqa: BLE001 - teardown must not mask failures
            tdLog.info(f"[tmq-teardown] drop database {cls.dbname} failed: {e}")
        tdLog.success(f"{__file__} successfully executed")
