# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

import pytest
import sys
import time
import random
import threading
import taos
from new_test_framework.utils import tdLog, tdSql, tdCom, cluster, sc, clusterComCheck


class TestShowAlive:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_show_alive(self):
        """Show alive

        Continuously start and stop multiple dnodes to verify the returned results of show alive.

        Catalog:
            - Show

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/show/showalive.sim

        """

        clusterComCheck.checkDnodes(4)
        clusterComCheck.checkClusterAlive(1, 20)

        tdLog.info(f"=============== create database, stable, table")
        tdSql.execute(f"create database test  vgroups 6;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"insert into t1 values(1648791211000,1,2,3);")
        tdSql.execute(f"insert into t1 values(1648791222001,2,2,3);")
        tdSql.execute(f"insert into t2 values(1648791211000,1,2,3);")
        tdSql.execute(f"insert into t2 values(1648791222001,2,2,3);")

        clusterComCheck.checkClusterAlive(1, 20)
        clusterComCheck.checkDbAlive("test", 1, 20)

        tdLog.info(f"stop dnode3")
        tdLog.info(f"stop dnode4")

        sc.dnodeForceStop(3)
        sc.dnodeForceStop(4)
        clusterComCheck.checkDnodes(2)

        clusterComCheck.checkClusterAlive(2, 20)
        clusterComCheck.checkDbAlive("test", 2, 20)

        tdSql.execute(f"create database test1 vgroups 2;")
        clusterComCheck.checkClusterAlive(2, 20)
        clusterComCheck.checkDbAlive("test", 2, 20)
        clusterComCheck.checkDbAlive("test1", 1, 20)

        tdLog.info(f"stop dnode2")
        sc.dnodeForceStop(2)
        clusterComCheck.checkDnodes(1)

        clusterComCheck.checkClusterAlive(2, 20)
        clusterComCheck.checkDbAlive("test", 2, 20)
        clusterComCheck.checkDbAlive("test1", 2, 20)

    def _restore_healthy_cluster(self):
        """Bring the shared cluster back to a known-good state.

        The deploy fixture is class-scoped, so tests in this class share one cluster and
        run in definition order; test_show_alive deliberately leaves it at 1 dnode with
        two degraded databases. The create-db tests below need 4 live dnodes (replica 3)
        and no pre-existing degraded vgroups, otherwise `show cluster alive` would report
        2 for reasons unrelated to the bug under test.
        """
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(4)
        for leftover in ["test", "test1"]:
            tdSql.execute(f"drop database if exists {leftover}")
        clusterComCheck.checkClusterAlive(1, 60)

    def test_create_db_alive_immediately(self):
        """Create database then show alive immediately

        After `create database` returns, `show cluster alive` and `show <db>.alive`
        must both return 1 on the very FIRST query, with no sleep and no polling.

        Rationale: the create-db transaction appends a TDMT_VND_ALTER_CONFIRM redo
        action per vgroup. That message is a sync write, so it only succeeds once the
        vgroup has elected a leader. The transaction therefore cannot reach the commit
        stage - which is where vgroups flip from SDB_STATUS_CREATING to
        SDB_STATUS_READY and thus become visible in information_schema.ins_vgroups -
        until every vgroup already has a leader.

        NOTE: this test deliberately does NOT use clusterComCheck.checkClusterAlive(),
        because that helper polls for up to `timeout` seconds and would happily
        mask the very timing window we are trying to assert away. tdSql.query() does
        have a queryTimes retry, but it only retries on exceptions, never on an
        unexpected result value, so the assertion below stays single-shot.

        Catalog:
            - Show

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-24 create db waits for vgroup leader election
        """
        self._restore_healthy_cluster()

        # Multiple rounds: leader election timing is inherently racy, so a single
        # round could pass by luck. Each round drops the db to keep the cluster clean.
        rounds = 5
        for i in range(rounds):
            dbname = f"db_alive_{i}"
            tdLog.info(f"round {i}: create database {dbname} replica 3 vgroups 4")

            start = time.time()
            tdSql.execute(f"create database {dbname} replica 3 vgroups 4")
            elapsed = time.time() - start
            # Observation only, NOT an assertion.
            tdLog.info(f"round {i}: create database {dbname} took {elapsed:.3f}s")

            # THE assertion: single-shot, no sleep, no retry.
            tdSql.query("show cluster alive")
            tdLog.info(f"round {i}: show cluster alive -> {tdSql.queryResult}")
            tdSql.checkData(0, 0, 1)

            tdSql.query(f"show {dbname}.alive")
            tdLog.info(f"round {i}: show {dbname}.alive -> {tdSql.queryResult}")
            tdSql.checkData(0, 0, 1)

            tdSql.execute(f"drop database {dbname}")

    def test_create_db_alive_from_other_client(self):
        """Create database on one client, observe show cluster alive on another

        A SECOND, independent connection polls `show cluster alive` as fast as it can
        while the first connection runs `create database ... replica 3`. That observer
        must never see 2.

        Why a second connection is required to expose the bug: mndTransSendRpcRsp()
        (mndTrans.c) waits up to 12 seconds for mndIsDbReady() before replying to the
        client that issued create-db. But that wait happens AFTER the commit stage has
        already flipped every vgroup to SDB_STATUS_READY (mndSetCreateDbCommitLogs in
        mndDb.c), which is the moment the vgroups become visible in
        information_schema.ins_vgroups to the whole cluster. So during that window the
        vgroups are visible but may still be leaderless, and `show cluster alive`
        returns 2 - for every client except the one being held back by the wait.
        A same-connection test is therefore structurally blind to this bug.

        With the alter-confirm redo action in place, leader election completes BEFORE
        commit, so the window is closed for all clients and this test passes.

        Catalog:
            - Show

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-24 create db waits for vgroup leader election
        """
        self._restore_healthy_cluster()

        # A pre-existing healthy database is required to reproduce the reported symptom
        # exactly. `show cluster alive` returns 2 only when SOME vgroups have a leader
        # and some do not. On an otherwise empty cluster the new db's vgroups are the
        # only ones, so a leaderless moment yields 0 instead of 2. Either value is a
        # bug, but the baseline db makes the observation match the reported 2.
        tdSql.execute("drop database if exists db_alive_base")
        tdSql.execute("create database db_alive_base replica 3 vgroups 2")
        clusterComCheck.checkClusterAlive(1, 20)

        # Independent connection: must NOT share tdSql, otherwise the observation is
        # serialized behind create-db and the racy window is never sampled.
        observer = tdCom.newTdSql()

        # tdLog.exit() below terminates the process, so cleanup has to be in `finally`
        # or the baseline db would be left behind for the next test in this class.
        failure = None
        stragglers = []
        try:
            rounds = 5
            for i in range(rounds):
                dbname = f"db_alive_obs_{i}"

                # Shared state between the observer thread and this thread.
                samples = []
                bad = []
                errors = []  # failed queries, tracked to detect a broken environment
                stop = threading.Event()

                # Bind the per-round state as default arguments rather than closing
                # over the loop variables, so each round's thread cannot touch the
                # next round's lists.
                def poll(rnd=i, samples=samples, bad=bad, errors=errors, stop=stop):
                    # Poll as tightly as possible; each sample is a fresh single-shot
                    # query, so a transient 2 is recorded rather than retried away.
                    while not stop.is_set():
                        try:
                            observer.query("show cluster alive", queryTimes=1)
                            # Read queryResult directly: getData() goes through
                            # checkRowCol(), which calls tdLog.exit() -> SystemExit,
                            # and SystemExit would kill this thread unnoticed.
                            result = observer.queryResult
                            if not result or not result[0]:
                                errors.append("empty result")
                            else:
                                value = result[0][0]
                                samples.append(value)
                                if value != 1:
                                    bad.append(value)
                        except Exception as e:
                            # A failing query is not the signal we are after; keep polling.
                            errors.append(str(e))
                            tdLog.info(f"round {rnd}: observer query failed: {e}")
                        time.sleep(0.02)

                watcher = threading.Thread(target=poll, daemon=True)
                watcher.start()

                tdLog.info(f"round {i}: create database {dbname} replica 3 vgroups 4")
                start = time.time()
                tdSql.execute(f"create database {dbname} replica 3 vgroups 4")
                elapsed = time.time() - start
                tdLog.info(f"round {i}: create database {dbname} took {elapsed:.3f}s")

                # Keep observing past the create-db return: before the fix the reply is
                # delayed only for the create-db client (mndTransSendRpcRsp waits up to
                # 12s), while other clients keep seeing the leaderless vgroups. Sample a
                # few seconds beyond the return to cover the tail of that window.
                time.sleep(3)
                stop.set()
                watcher.join(10)
                if watcher.is_alive():
                    # A straggler thread still owns `observer`; closing it in the finally
                    # block would pull the connection out from under it.
                    stragglers.append(watcher)
                    tdLog.warning(f"round {i}: observer thread did not stop within 10s")

                tdLog.info(
                    f"round {i}: observer took {len(samples)} samples, {len(errors)} failed, "
                    f"{len(bad)} not equal to 1: {bad[:20]}"
                )

                tdSql.execute(f"drop database {dbname}")

                # An observer that mostly failed proves nothing either way; say so rather
                # than reporting a misleading pass.
                if len(errors) >= len(samples):
                    failure = (
                        f"round {i}: observer is unreliable, {len(errors)} failed vs "
                        f"{len(samples)} good samples, first errors: {errors[:3]}"
                    )
                    break

                if len(bad) > 0:
                    failure = (
                        f"round {i}: another client saw show cluster alive = {bad[0]} "
                        f"while database {dbname} was being created "
                        f"({len(bad)} of {len(samples)} samples)"
                    )
                    break
        finally:
            # Only safe to close once no poll thread can still be using the connection.
            if not stragglers:
                observer.close()
            try:
                tdSql.execute("drop database if exists db_alive_base")
            except Exception as e:
                # Never let cleanup mask the failure we are actually reporting.
                tdLog.info(f"cleanup of db_alive_base failed: {e}")

        if failure is not None:
            tdLog.exit(failure)

    def test_create_db_alive_replica_and_vgroup_matrix(self):
        """Create database then show alive across replica and vgroups combinations

        Covers the design doc matrix: single replica (mndIsDbReady short-circuits and no
        election is needed), three replicas (the reported scenario), one vgroup (a single
        redo action group) and many vgroups (proves TRN_EXEC_GROUP_PARALLEL still elects
        leaders concurrently across groups instead of serializing them).

        Catalog:
            - Show

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-24 coverage matrix for create-db wait-for-leader
        """
        self._restore_healthy_cluster()

        # (dbname, replica, vgroups)
        matrix = [
            ("db_r1_v1", 1, 1),
            ("db_r1_v4", 1, 4),
            ("db_r3_v1", 3, 1),
            ("db_r3_v10", 3, 10),
        ]

        for dbname, replica, vgroups in matrix:
            tdLog.info(f"create database {dbname} replica {replica} vgroups {vgroups}")
            start = time.time()
            tdSql.execute(f"create database {dbname} replica {replica} vgroups {vgroups}")
            elapsed = time.time() - start
            # Observation only: replica 1 is expected to be far faster than replica 3,
            # since no leader election round trip is involved.
            tdLog.info(
                f"{dbname}: replica={replica} vgroups={vgroups} create took {elapsed:.3f}s"
            )

            # Single-shot, no sleep, no polling - same reasoning as
            # test_create_db_alive_immediately.
            tdSql.query("show cluster alive")
            tdLog.info(f"{dbname}: show cluster alive -> {tdSql.queryResult}")
            tdSql.checkData(0, 0, 1)

            tdSql.query(f"show {dbname}.alive")
            tdLog.info(f"{dbname}: show {dbname}.alive -> {tdSql.queryResult}")
            tdSql.checkData(0, 0, 1)

            # Drop before the next combination so a lingering degraded vgroup from this
            # database cannot make the next assertion report 2.
            tdSql.execute(f"drop database {dbname}")
