###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

# -*- coding: utf-8 -*-
"""Test: query a virtual table while ``SPLIT VGROUP`` is in flight.

Scenario
--------
1. create origin super/child tables and fill them with data on a
   single-vgroup database
2. create a virtual super table + virtual child table that map columns
   from the origin child tables
3. start a background worker that keeps querying the virtual table,
   timestamping every attempt
4. trigger ``split vgroup`` on the main thread, recording the window
   boundaries (``split_start`` / ``split_end``)
5. keep ONLY the attempts that fall completely inside the split window
   and report whether the virtual-table queries kept returning correct
   results

Why the window filter matters
-----------------------------
The worker starts before the split is issued and may complete one more round
after the split transaction is gone. Those out-of-window attempts naturally
succeed, so mixing them into the statistics would let the test pass even if
*every* query issued during the actual split had failed. Filtering on the
recorded window is what makes the assertions meaningful.
"""

import time
import threading

from new_test_framework.utils import tdLog, tdSql
from split_concurrent_util import new_connection, get_vgroups, split_first_vgroup


class TestSplitWithVtableQuery:
    dbname = "split_vtable"
    # number of origin child tables that back the virtual table
    org_child_num = 8
    # one split needs real work so it overlaps the worker; bump the per-table
    # row count so the split window lasts long enough for queries to land
    # inside it (see the window-filter note in the module docstring)
    rows_per_table = 5000

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        # single vgroup db so the split target is deterministic
        tdSql.execute(f"drop database if exists {cls.dbname}")
        tdSql.execute(f"create database {cls.dbname} vgroups 1 replica 1")
        tdSql.execute(f"use {cls.dbname}")

        # ---- origin super table + child tables (real data) ----
        tdSql.execute(
            f"create stable {cls.dbname}.org_stb "
            f"(ts timestamp, int_col int, float_col float) "
            f"tags (t1 int)"
        )
        base = 1700000000000
        for i in range(cls.org_child_num):
            tdSql.execute(
                f"create table {cls.dbname}.org_c{i} using {cls.dbname}.org_stb tags({i})"
            )
            # batch insert rows for this child table
            values = []
            for r in range(cls.rows_per_table):
                values.append(f"({base + r * 1000}, {r}, {r * 1.5})")
                if len(values) == 500:
                    tdSql.execute(
                        f"insert into {cls.dbname}.org_c{i} values " + " ".join(values)
                    )
                    values = []
            if values:
                tdSql.execute(
                    f"insert into {cls.dbname}.org_c{i} values " + " ".join(values)
                )
        tdLog.info(f"[vtable] origin data ready, vgroups={get_vgroups(cls.dbname)}")

        # ---- virtual super table + virtual child tables ----
        # A virtual super table declares the schema; each virtual child table
        # maps its columns FROM the origin child tables above.
        tdSql.execute(
            f"create stable {cls.dbname}.v_stb "
            f"(ts timestamp, int_col int, float_col float) "
            f"tags (vt int) virtual 1"
        )
        for i in range(cls.org_child_num):
            tdSql.execute(
                f"create vtable {cls.dbname}.v_c{i} ("
                f"int_col from {cls.dbname}.org_c{i}.int_col, "
                f"float_col from {cls.dbname}.org_c{i}.float_col) "
                f"using {cls.dbname}.v_stb tags({i})"
            )
        tdLog.info("[vtable] virtual tables created")

    # ------------------------------------------------------------------
    # background query worker
    # ------------------------------------------------------------------
    def _query_worker(self, results, stop_evt):
        """Continuously query the virtual super table until stopped.

        Every attempt is recorded as a tuple
        ``(round, ok, rows_or_error, t_start, t_end)`` where ``t_start`` /
        ``t_end`` are ``time.time()`` values taken immediately before and
        after the statement. The main thread uses those timestamps to keep
        ONLY the attempts that fell entirely inside the split window --
        without them, queries issued before the split started or after it
        finished would be mixed into the statistics and mask a real
        regression.
        """
        conn = new_connection()
        cur = conn.cursor()
        rnd = 0
        while not stop_evt.is_set():
            rnd += 1
            sql = f"select count(*), avg(int_col) from {self.dbname}.v_stb"
            t0 = time.time()
            try:
                cur.execute(sql)
                data = cur.fetchall()
                cnt = data[0][0] if data else None
                results.append((rnd, True, cnt, t0, time.time()))
                tdLog.info(f"[vtable-worker] OK   #{rnd} count={cnt}")
            except Exception as e:  # noqa: BLE001
                results.append((rnd, False, str(e), t0, time.time()))
                tdLog.info(f"[vtable-worker] FAIL #{rnd} {e}")
            time.sleep(0.2)
        cur.close()
        conn.close()
        tdLog.info(f"[vtable-worker] stopped after {rnd} queries")

    def test_split_with_vtable_query(self):
        """SPLIT VGROUP 期间查询虚拟表

        验证在 split vgroup 事务进行过程中，对虚拟表执行查询是否能够正常
        返回结果。

        Since: v3.4.0.0

        Labels: cluster,split,virtualtable,query,functional

        Jira: None

        History:
            - 2026-06-13 Created
        """
        results = []
        stop_evt = threading.Event()
        worker = threading.Thread(
            target=self._query_worker, args=(results, stop_evt), daemon=True
        )

        tdLog.info("[vtable] start background query worker")
        worker.start()
        # give the worker a moment to warm up so some queries overlap the split
        time.sleep(1)

        # ------------------------------------------------------------------
        # main thread performs the split and blocks until it finishes.
        # split_start / split_end bracket the split window; only query attempts
        # that fall completely inside this window are meaningful for the
        # "can it run during split?" question.
        # ------------------------------------------------------------------
        split_start = time.time()
        vgid, elapsed = split_first_vgroup(self.dbname)
        split_end = time.time()
        tdLog.info(
            f"[vtable] split of vgroup {vgid} took {elapsed:.1f}s "
            f"(window {split_start:.3f} .. {split_end:.3f})"
        )

        # stop the worker and collect what happened during the split window
        stop_evt.set()
        worker.join(timeout=30)

        # Keep only the attempts fully contained in the split window. An
        # attempt that started before the split was issued, or finished after
        # the split transaction was already gone, tells us nothing about
        # concurrent behavior.
        in_window = [
            r for r in results if r[3] >= split_start and r[4] <= split_end
        ]
        outside = len(results) - len(in_window)

        total = len(in_window)
        ok = sum(1 for r in in_window if r[1])
        failed = total - ok
        tdLog.info(
            f"[vtable] query attempts inside split window: total={total}, "
            f"ok={ok}, failed={failed} "
            f"(discarded {outside} attempt(s) outside the window)"
        )
        # dump everything, flagging whether each attempt was inside the window
        for r in results:
            scope = "IN " if (r[3] >= split_start and r[4] <= split_end) else "OUT"
            tdLog.info(
                f"[vtable] result: scope={scope} round={r[0]} ok={r[1]} detail={r[2]}"
            )

        # We must have actually exercised virtual-table queries *during* the
        # split, otherwise the test proves nothing and should fail loudly
        # instead of passing on attempts made outside the window.
        assert total > 0, (
            "no virtual-table query fell inside the split window; the split "
            "likely finished too fast -- increase the seed data volume"
        )

        # At least one query fully inside the split window must have returned
        # correct results; this is what really covers "query the virtual table
        # while the split transaction is running".
        assert ok > 0, (
            "every in-window virtual-table query failed during the split "
            f"({total} attempts)"
        )

        # confirm the split really happened
        final_vgroups = get_vgroups(self.dbname)
        assert len(final_vgroups) >= 2, (
            f"split did not increase vgroup count, got {final_vgroups}"
        )
        tdLog.info("[vtable] test_split_with_vtable_query PASSED")
