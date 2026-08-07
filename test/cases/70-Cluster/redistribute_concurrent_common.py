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
"""Shared helpers for the "redistribute vgroup + concurrent feature" tests.

These tests verify that while a `REDISTRIBUTE VGROUP` transaction is running
(moving a vnode from one dnode to another), other TDengine features keep
working correctly:

  - stream processing
  - data subscription (TMQ)
  - virtual table query
  - metadata operations (e.g. create super table)

The common flow of every test case is:

  1. Deploy a multi-dnode cluster (dnode1 is mnode-only, data lives on the
     other dnodes) and insert a batch of data.
  2. Continuously run `REDISTRIBUTE VGROUP` in a background thread so a
     redistribute transaction is (almost) always in-flight.
  3. In parallel, exercise the target feature and verify it behaves correctly.
  4. Stop the background redistribute thread and clean up.

This module provides `RedistributeConcurrentBase`, a mixin base class that
implements the cluster setup, data preparation and the background
redistribute driver. Each concrete test subclasses it and implements only the
feature-specific verification.
"""

import threading
import time

from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdCom,
    sc,
    clusterComCheck,
)

# `REDISTRIBUTE VGROUP` creates an mnode transaction with a GLOBAL conflict
# scope (see mndVgroup.c: TRN_CONFLICT_GLOBAL). While such a transaction is
# in-flight, any OTHER mnode DDL transaction (create/alter/drop stable, create
# stream, create/drop topic, ...) is rejected by the server with
# `TSDB_CODE_MND_TRANS_CONFLICT (0x03D3)` whose message is
# "Conflict transaction not completed". This is the PRODUCT'S NORMAL behavior,
# not a bug: the metadata operation just needs to be retried until it lands in
# a window where no conflicting transaction is running.
#
# The substrings below are used to recognize that transient conflict so the
# tests can retry the DDL instead of failing. We match on both the numeric code
# and the human-readable message to be robust across connector versions.
TRANS_CONFLICT_MARKERS = (
    "0x3d3",                          # error code, lower-case hex
    "conflict transaction not completed",
    "transaction not completed",     # compact/retention conflict variants
    "conflict",                      # generic fallback
)


def _is_trans_conflict_error(err):
    """Return True if `err` looks like an mnode transaction-conflict error.

    Such errors are expected while a redistribute transaction is running and
    should be retried rather than treated as a test failure.
    """
    msg = str(err).lower()
    return any(marker in msg for marker in TRANS_CONFLICT_MARKERS)


class RedistributeConcurrentBase:
    """Base class shared by all "redistribute + concurrent feature" tests.

    Subclasses are expected to set (as class attributes, before setup):
      - dnodeNum:   total dnodes in the cluster (default 4)
      - dataDnodes: list of dnode ids that host vnodes (default [2, 3, 4])

    dnode1 is always turned into an mnode-only node (supportVnodes = 0) so the
    redistribute target selection is simple and deterministic.
    """

    # Total number of dnodes deployed by the cluster (see -N in cases.task).
    dnodeNum = 4
    # dnode ids that are allowed to host vnodes. dnode1 is mnode-only.
    dataDnodes = [2, 3, 4]
    # Idle gap (seconds) inserted after each redistribute transaction finishes
    # before the next one starts.  REDISTRIBUTE VGROUP creates a
    # TRN_CONFLICT_GLOBAL mnode transaction that blocks ALL other mnode DDL
    # (create/alter/drop stable, create topic/stream, ...) with error 0x03D3
    # "Conflict transaction not completed".  A back-to-back redistribute loop
    # would keep the global conflict lock held almost continuously, leaving no
    # window for concurrent mnode DDL to ever succeed.  This gap models a
    # realistic operations scenario (redistribute does not occupy 100% of the
    # timeline) and gives the concurrent DDL's retry logic a window to land in.
    redistributeGap = 3

    # ------------------------------------------------------------------
    # Cluster / data preparation
    # ------------------------------------------------------------------
    def prepare_cluster(self):
        """Wait for all dnodes to be ready and make dnode1 mnode-only.

        Making dnode1 host no vnodes keeps the set of candidate dnodes for a
        replica-1 vgroup restricted to `self.dataDnodes`, which makes the
        redistribute target selection trivial and avoids moving data onto the
        management node.
        """
        tdLog.info(f"prepare cluster: wait {self.dnodeNum} dnodes ready")
        clusterComCheck.checkDnodes(self.dnodeNum)

        tdLog.info("set dnode 1 supportVnodes = 0 (mnode-only)")
        tdSql.execute("alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)

    def prepare_data(self, dbname, stbname="stb", ctbNum=10, rowsPerTbl=100,
                     vgroups=2, replica=1):
        """Create a database + super table + child tables and insert data.

        Returns the total number of rows inserted so callers can verify data
        integrity later.
        """
        tdLog.info(f"prepare data: db={dbname} vgroups={vgroups} replica={replica}")
        tdSql.execute(f"drop database if exists {dbname}")
        # wal_retention_period is set so TMQ can consume from WAL reliably.
        tdSql.execute(
            f"create database {dbname} vgroups {vgroups} replica {replica} "
            f"wal_retention_period 3600"
        )
        tdSql.execute(f"use {dbname}")
        tdSql.execute(
            f"create stable {dbname}.{stbname} "
            f"(ts timestamp, c1 int, c2 float) tags (t1 int)"
        )

        startTs = 1640966400000  # 2022-01-01 00:00:00.000
        for i in range(ctbNum):
            tdSql.execute(
                f"create table {dbname}.ctb{i} using {dbname}.{stbname} tags({i})"
            )
            # Build one multi-row insert per child table to speed things up.
            values = []
            for r in range(rowsPerTbl):
                values.append(f"({startTs + r * 1000}, {r}, {r * 1.0})")
            tdSql.execute(
                f"insert into {dbname}.ctb{i} values {' '.join(values)}"
            )

        totalRows = ctbNum * rowsPerTbl
        tdLog.info(f"prepared {totalRows} rows in {dbname}.{stbname}")
        return totalRows

    # ------------------------------------------------------------------
    # Conflict-tolerant metadata execution
    # ------------------------------------------------------------------
    def execute_with_conflict_retry(self, atdSql, sql, timeout=180, interval=1):
        """Execute an mnode DDL that may conflict with a redistribute trans.

        While `REDISTRIBUTE VGROUP` is running, any other mnode DDL (create
        stable / stream / topic, alter stable, ...) can be rejected with
        `TSDB_CODE_MND_TRANS_CONFLICT (0x03D3)` — "Conflict transaction not
        completed". This is expected: the redistribute transaction holds a
        GLOBAL conflict lock. The correct behavior is to RETRY the DDL until it
        succeeds in a window where no conflicting transaction is running.

        This helper keeps retrying `sql` on transaction-conflict errors until it
        succeeds or `timeout` seconds elapse. Non-conflict errors are raised
        immediately so real failures are not masked.

        Args:
            atdSql: a tdSql-like connection (use tdCom.newTdSql() in threads).
            sql:     the DDL statement to execute.
            timeout: max seconds to keep retrying on conflict.
            interval: seconds to wait between retries.

        Returns:
            True when the statement eventually executes successfully.

        Raises:
            The original exception for non-conflict errors, or a timeout error
            if the conflict never clears within `timeout` seconds.
        """
        deadline = time.time() + timeout
        attempt = 0
        while True:
            attempt += 1
            try:
                # queryTimes=1 so the framework does not swallow/retry the
                # conflict itself; we manage retries here and log each attempt.
                atdSql.execute(sql, queryTimes=1)
                if attempt > 1:
                    tdLog.info(
                        f"DDL succeeded after {attempt} attempts: {sql}"
                    )
                return True
            except Exception as e:  # noqa: BLE001
                if not _is_trans_conflict_error(e):
                    # A genuine error (syntax, already-exists, ...) — surface it.
                    tdLog.info(f"DDL failed with non-conflict error: {sql} -> {e}")
                    raise
                if time.time() >= deadline:
                    raise Exception(
                        f"DDL still conflicting after {timeout}s "
                        f"({attempt} attempts): {sql} -> {e}"
                    )
                # Expected transient conflict with the redistribute trans; wait
                # and retry so we validate the feature eventually succeeds.
                tdLog.info(
                    f"DDL conflicts with redistribute trans, retry "
                    f"#{attempt}: {sql}"
                )
                time.sleep(interval)

    def call_with_conflict_retry(self, func, desc, timeout=180, interval=1):
        """Call an arbitrary callable, retrying on transaction-conflict errors.

        Some client operations are NOT plain SQL going through tdSql.execute but
        still trigger an mnode transaction under the hood.  A concrete example
        is `Consumer.subscribe()`: the FIRST subscribe of a consumer group has
        to create the subscription/consumer-group metadata on the mnode, which
        is a mnode DDL transaction and therefore conflicts with an in-flight
        redistribute (GLOBAL) transaction, failing with 0x03D3
        "Conflict transaction not completed".

        This helper retries `func()` on such transient conflicts until it
        succeeds or `timeout` seconds elapse.  Non-conflict errors are raised
        immediately so real failures are not masked.

        Args:
            func:    a zero-argument callable to invoke (e.g. a lambda wrapping
                     consumer.subscribe([topic])).
            desc:    a short human-readable description used in log messages.
            timeout: max seconds to keep retrying on conflict.
            interval: seconds to wait between retries.

        Returns:
            Whatever `func()` returns once it eventually succeeds.
        """
        deadline = time.time() + timeout
        attempt = 0
        while True:
            attempt += 1
            try:
                result = func()
                if attempt > 1:
                    tdLog.info(f"{desc} succeeded after {attempt} attempts")
                return result
            except Exception as e:  # noqa: BLE001
                if not _is_trans_conflict_error(e):
                    # A genuine error — surface it immediately.
                    tdLog.info(f"{desc} failed with non-conflict error: {e}")
                    raise
                if time.time() >= deadline:
                    raise Exception(
                        f"{desc} still conflicting after {timeout}s "
                        f"({attempt} attempts): {e}"
                    )
                # Expected transient conflict with the redistribute trans.
                tdLog.info(
                    f"{desc} conflicts with redistribute trans, retry "
                    f"#{attempt}"
                )
                time.sleep(interval)

    # ------------------------------------------------------------------
    # Redistribute driver (runs in a background thread)
    # ------------------------------------------------------------------
    def _get_vgroup_dnode(self, atdSql, dbname, vgId):
        """Return the dnode id currently hosting the (replica-1) vgroup."""
        atdSql.query(
            f"select dnode_id from information_schema.ins_vnodes "
            f"where db_name='{dbname}' and vgroup_id={vgId}"
        )
        if atdSql.queryRows > 0:
            return atdSql.queryResult[0][0]
        return -1

    def _wait_transaction_zero(self, atdSql, seconds=120, interval=1):
        """Wait until there is no in-flight transaction (or timeout)."""
        for _ in range(seconds):
            rows = atdSql.query("show transactions")
            if rows == 0:
                return True
            time.sleep(interval)
        return False

    def redistribute_loop(self, dbname, stop_event, error_holder):
        """Continuously redistribute vgroups of `dbname` until stop_event set.

        For every vgroup of the database, this repeatedly moves the vnode to a
        different data dnode and waits for the redistribute transaction to
        finish, so that during the whole feature test there is a steady stream
        of redistribute transactions in the cluster.

        Any exception is stored in `error_holder` so the main thread can fail
        the test; the loop itself never raises.
        """
        newTdSql = tdCom.newTdSql()
        loop = 0
        try:
            while not stop_event.is_set():
                # Collect the current vgroup ids of the database.
                newTdSql.query(
                    f"select vgroup_id from information_schema.ins_vgroups "
                    f"where db_name='{dbname}'"
                )
                vgroups = [row[0] for row in newTdSql.queryResult]
                if not vgroups:
                    tdLog.info(f"redistribute_loop: no vgroups for {dbname} yet")
                    time.sleep(1)
                    continue

                for vgId in vgroups:
                    if stop_event.is_set():
                        break
                    curDnode = self._get_vgroup_dnode(newTdSql, dbname, vgId)
                    # Pick a target dnode different from the current one.
                    candidates = [d for d in self.dataDnodes if d != curDnode]
                    if not candidates:
                        continue
                    target = candidates[loop % len(candidates)]

                    sql = f"redistribute vgroup {vgId} dnode {target}"
                    tdLog.info(f"redistribute_loop[{loop}]: {sql} (cur={curDnode})")
                    newTdSql.execute(sql)
                    if self._wait_transaction_zero(newTdSql) is False:
                        raise Exception(
                            f"redistribute transaction not finished for {sql}"
                        )
                    loop += 1
                    # Idle gap AFTER the redistribute transaction has fully
                    # finished (show transactions == 0).  During this window the
                    # global conflict lock is released, so concurrent mnode DDL
                    # (create stable / alter stable / ...) can grab its own
                    # transaction and succeed.  Without this gap the next
                    # redistribute would start immediately and the DDL would
                    # keep hitting 0x03D3 forever.  We still re-check stop_event
                    # so shutdown stays responsive.
                    if self.redistributeGap > 0 and not stop_event.is_set():
                        tdLog.info(
                            f"redistribute_loop[{loop}]: idle gap "
                            f"{self.redistributeGap}s to allow concurrent DDL"
                        )
                        stop_event.wait(self.redistributeGap)
                # Small pause to avoid an overly tight loop.
                time.sleep(0.2)
        except Exception as e:  # noqa: BLE001 - propagate to main thread
            tdLog.info(f"redistribute_loop error: {e}")
            error_holder.append(str(e))
        finally:
            tdLog.info(f"redistribute_loop stopped after {loop} redistributes")

    def start_redistribute(self, dbname):
        """Start the background redistribute thread.

        Returns (thread, stop_event, error_holder). Call stop_redistribute()
        to join and check for errors.
        """
        stop_event = threading.Event()
        error_holder = []
        thread = threading.Thread(
            target=self.redistribute_loop,
            args=(dbname, stop_event, error_holder),
            daemon=True,
        )
        thread.start()
        tdLog.info("background redistribute thread started")
        # Give the loop a moment to kick off the first redistribute so the
        # feature under test really overlaps with an in-flight transaction.
        time.sleep(2)
        return thread, stop_event, error_holder

    def stop_redistribute(self, thread, stop_event, error_holder):
        """Stop the background redistribute thread and fail on any error."""
        tdLog.info("stopping background redistribute thread")
        stop_event.set()
        thread.join(timeout=180)
        if thread.is_alive():
            tdLog.exit("redistribute thread did not stop within 180s")
        if error_holder:
            tdLog.exit(f"redistribute thread reported error: {error_holder[0]}")
        tdLog.info("background redistribute thread stopped cleanly")
