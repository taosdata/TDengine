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
"""Shared helpers for the "split vgroup + concurrent operation" test suite.

These four tests all follow the same skeleton:

  1. prepare data on a database that has exactly one vgroup
  2. launch a background worker that keeps running some operation
     (stream / subscription / virtual-table query / metadata DDL)
  3. trigger ``SPLIT VGROUP`` on the main thread and wait until the
     background split transaction finishes
  4. inspect the results the worker recorded *while the split was in
     flight* so the test report can state which operations are allowed
     during a split and which are rejected.

The helpers below centralize the connection factory and the split
driver so each test file stays focused on its own concurrent operation.
"""

import time
import taos

from new_test_framework.utils import tdLog, tdSql


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------
def new_connection(host="localhost", port=6030):
    """Create a dedicated native connection.

    Every background worker needs its OWN connection object because a single
    ``taos`` connection is not safe to share across threads. Callers must
    close the returned connection when they are done.
    """
    tdLog.info(f"[split-util] open new connection to {host}:{port}")
    conn = taos.connect(host=host, user="root", password="taosdata", port=port)
    return conn


# ---------------------------------------------------------------------------
# vgroup / split helpers
# ---------------------------------------------------------------------------
def get_vgroups(db_name):
    """Return the list of vgroup ids that belong to ``db_name``."""
    tdSql.query(
        f"select vgroup_id from information_schema.ins_vgroups "
        f"where db_name='{db_name}'"
    )
    vgroups = [row[0] for row in tdSql.queryResult]
    tdLog.info(f"[split-util] db={db_name} current vgroups={vgroups}")
    return vgroups


def wait_transactions_clear(timeout=300):
    """Block until ``show transactions`` reports no in-flight transaction.

    The split is implemented as an mnode transaction. We treat the split as
    "finished" once the transaction table is empty again. Returns the number
    of seconds waited so the test can log the split duration.
    """
    tdLog.info("[split-util] waiting for split transaction to finish ...")
    start = time.time()
    while True:
        tdSql.query("show transactions")
        pending = tdSql.getRows()
        elapsed = time.time() - start
        if pending == 0:
            tdLog.info(f"[split-util] no pending transaction, split done in {elapsed:.1f}s")
            return elapsed
        if elapsed > timeout:
            tdLog.exit(f"[split-util] split transaction not finished within {timeout}s")
        tdLog.info(f"[split-util] {pending} transaction(s) still running, waited {elapsed:.1f}s")
        time.sleep(1)


def split_first_vgroup(db_name, force=False):
    """Trigger ``SPLIT VGROUP`` on the first vgroup of ``db_name``.

    ``force=True`` appends the ``FORCE`` keyword, which tells the mnode to go
    ahead even when a virtual-table stream exists in the cluster.

    Returns a tuple ``(vgroup_id, elapsed_seconds)``. The caller is expected
    to have generated enough data beforehand so the split has real work to do
    (and therefore overlaps with the concurrent worker for a while).
    """
    vgroups = get_vgroups(db_name)
    if not vgroups:
        tdLog.exit(f"[split-util] db {db_name} has no vgroup to split")

    target = vgroups[0]
    stmt = f"split vgroup {target} force" if force else f"split vgroup {target}"
    tdLog.info(f"[split-util] >>> executing: {stmt}")
    tdSql.execute(stmt)
    elapsed = wait_transactions_clear()

    after = get_vgroups(db_name)
    tdLog.info(
        f"[split-util] split finished: vgroups {vgroups} -> {after} "
        f"(count {len(vgroups)} -> {len(after)})"
    )
    return target, elapsed


def trigger_split_first_vgroup(db_name):
    """Issue ``SPLIT VGROUP`` and return immediately, WITHOUT waiting.

    ``split vgroup`` only *creates* the mnode transaction; the actual vnode
    data movement continues asynchronously afterwards. This helper is for
    tests that need to run something else *while the split transaction is
    still in flight*: it fires the split and hands control straight back to
    the caller, who is responsible for calling
    :func:`wait_transactions_clear` later.

    Returns the vgroup id that was asked to split.
    """
    vgroups = get_vgroups(db_name)
    if not vgroups:
        tdLog.exit(f"[split-util] db {db_name} has no vgroup to split")

    target = vgroups[0]
    tdLog.info(f"[split-util] >>> triggering (no-wait): split vgroup {target}")
    tdSql.execute(f"split vgroup {target}")
    tdLog.info(
        f"[split-util] split vgroup {target} submitted, transaction now in flight"
    )
    return target


def transactions_pending():
    """Return the number of in-flight mnode transactions.

    Used to tell whether the split transaction is still running, so a test
    can scope its concurrent operations to exactly the split window.
    """
    tdSql.query("show transactions")
    return tdSql.getRows()


def try_split_first_vgroup(db_name, force=False):
    """Attempt ``SPLIT VGROUP`` but do NOT retry / assert on failure.

    Unlike :func:`split_first_vgroup`, this issues the split through a raw
    connection cursor so a rejection surfaces immediately as an exception
    instead of being retried by the framework's ``tdSql.execute`` wrapper.

    ``force=True`` appends the ``FORCE`` keyword so the caller can compare the
    plain and the forced behaviour on the very same topology.

    Returns ``(target_vgroup_id, ok, err_text)`` where ``ok`` is ``True`` when
    the statement was accepted. This is used by the stream test to prove that
    a split is *rejected* while a stream is using the database.
    """
    vgroups = get_vgroups(db_name)
    if not vgroups:
        tdLog.exit(f"[split-util] db {db_name} has no vgroup to split")

    target = vgroups[0]
    stmt = f"split vgroup {target} force" if force else f"split vgroup {target}"
    try:
        conn = new_connection()
        cur = conn.cursor()
        tdLog.info(f"[split-util] >>> try (no-retry): {stmt}")
        cur.execute(stmt)
        tdLog.info(f"[split-util] '{stmt}' was ACCEPTED")
        result = (target, True, "")
    except Exception as e:  # noqa: BLE001 - keep the raw error for the report
        tdLog.info(f"[split-util] '{stmt}' was REJECTED: {e}")
        result = (target, False, str(e))
    finally:
        cur.close()
        conn.close()
    return result
