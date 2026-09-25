# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

from new_test_framework.utils import tdCom, tdLog, tdSql, etool, cluster

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import os
import subprocess
import time
import uuid

import taos
import pytest


class TestCorrelatedScalarLifecycle:
    updatecfgDict = {"numOfVnodeQueryThreads": 1, "numOfQnodeQueryThreads": 1}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def do_late_error(self):
        scalar = "(select h.v from history h where h.device = r.device)"
        tdSql.error(
            f"select r.device, {scalar} from readings r order by r.ts",
            expectedErrno=0x26A5,
        )
        tdSql.query(
            f"select r.device, {scalar} from readings r "
            "where r.device < 3 order by r.ts",
            queryTimes=1,
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 11)
        tdSql.checkData(1, 1, 22)
        print("late cardinality error and next query ... [ passed ]")

    def do_concurrent_roots(self, database):
        barrier = Barrier(3)

        def query():
            conn = taos.connect(host=self.host, port=self.port, config=self.cfg_path, database=database)
            try:
                cursor = conn.cursor()
                try:
                    barrier.wait(timeout=15)
                    cursor.execute(
                        "select r.device, (select max(h.v) from history h "
                        "where h.device = r.device) from readings r "
                        "where r.device < 3 order by r.ts"
                    )
                    return cursor.fetchall()
                finally:
                    cursor.close()
            finally:
                conn.close()

        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(query) for _ in range(3)]
            for future in futures:
                assert future.result(timeout=60) == [(1, 11), (2, 22)]
        print("concurrent independent roots ............ [ passed ]")

    def do_killed_query(self, database):
        for retry_same_connection, sql in [
            (
                True,
                "select r.device, (select sleep(15) from "
                f"{database}.history h where h.device = r.device) "
                f"from {database}.readings r where r.device = 1",
            ),
            (
                False,
                "select r.device, (select (select sleep(15) from "
                f"{database}.history h2 where h2.device = h.device) "
                f"from {database}.history h where h.device = r.device) "
                f"from {database}.readings r where r.device = 1",
            ),
        ]:
            def query():
                conn = taos.connect(host=self.host, port=self.port, config=self.cfg_path)
                try:
                    cursor = conn.cursor()
                    try:
                        cursor.execute(sql)
                        return True, cursor.fetchall(), None
                    except Exception as exc:
                        error = str(exc)
                        if retry_same_connection:
                            cursor.execute(sql)
                            return False, error, cursor.fetchall()
                        return False, error, None
                    finally:
                        cursor.close()
                finally:
                    conn.close()

            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(query)
                query_id = None
                deadline = time.monotonic() + 8
                while time.monotonic() < deadline and not future.done() and query_id is None:
                    time.sleep(0.1)
                    tdSql.query("select kill_id, sql from performance_schema.perf_queries", queryTimes=1)
                    for kill_id, running_sql in tdSql.queryResult:
                        if str(running_sql).strip() == sql:
                            query_id = kill_id
                            break
                assert query_id is not None, "correlated query did not appear in perf_queries"
                time.sleep(1)
                assert not future.done(), "correlated query finished before cancellation"
                tdSql.execute(f"kill query '{query_id}'")
                succeeded, result, retry_rows = future.result(timeout=35 if retry_same_connection else 10)
                assert not succeeded and "killed" in result.lower(), result
                if retry_same_connection:
                    assert len(retry_rows) == 1 and retry_rows[0][0] == 1, retry_rows
            tdSql.query(
                "select r.device, (select max(h.v) from history h where h.device = r.device) "
                "from readings r where r.device = 1",
                queryTimes=1,
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 11)
        print("killed correlated query ................. [ passed ]")

    def do_client_disconnect(self, database):
        sql = (
            "select r.device, (select sleep(30) from "
            f"{database}.history h where h.device = r.device) "
            f"from {database}.readings r where r.device = 1"
        )
        bin_dir = os.environ.get("TAOS_BIN_PATH")
        if bin_dir is None:
            bin_dir = os.path.join(tdCom.getBuildPath(), "build", "bin")
        process = subprocess.Popen(
            [os.path.join(bin_dir, "taos"), "-c", self.cfg_path, "-h", self.host,
             "-P", str(self.port), "-s", sql],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        query_id = None
        try:
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline and process.poll() is None and query_id is None:
                tdSql.query("select kill_id, sql from performance_schema.perf_queries", queryTimes=1)
                for kill_id, running_sql in tdSql.queryResult:
                    if str(running_sql).strip() == sql:
                        query_id = kill_id
                        break
                if query_id is None:
                    time.sleep(0.1)
            assert query_id is not None, "correlated query did not start before client disconnect"
            process.terminate()
            process.wait(timeout=5)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                tdSql.query("select kill_id, sql from performance_schema.perf_queries", queryTimes=1)
                if all(kill_id != query_id for kill_id, _ in tdSql.queryResult):
                    break
                time.sleep(0.2)
            else:
                tdSql.execute(f"kill query '{query_id}'")
                assert False, "correlated query remained active after client disconnect"
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
        tdSql.query(
            "select r.device, (select max(h.v) from history h "
            "where h.device = r.device) from readings r where r.device = 1",
            queryTimes=1,
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 11)
        print("client disconnect releases active child ... [ passed ]")

    def do_lifecycle(self):
        database = f"correlated_scalar_e5_{uuid.uuid4().hex[:8]}"
        tdSql.execute(f"create database {database}")
        try:
            tdSql.execute(f"use {database}")
            tdSql.execute("create table readings (ts timestamp, device int)")
            tdSql.execute("create table history (ts timestamp, device int, v int)")
            tdSql.execute(
                "insert into readings values "
                "('2026-09-11 00:00:01', 1) "
                "('2026-09-11 00:00:02', 2) "
                "('2026-09-11 00:00:03', 3)"
            )
            tdSql.execute(
                "insert into history values "
                "('2026-09-11 00:00:01', 1, 11) "
                "('2026-09-11 00:00:02', 2, 22) "
                "('2026-09-11 00:00:03', 3, 33) "
                "('2026-09-11 00:00:04', 3, 34)"
            )
            self.do_late_error()
            self.do_concurrent_roots(database)
            self.do_killed_query(database)
            self.do_client_disconnect(database)
        finally:
            tdSql.execute(f"drop database {database}")

    def do_inner_node_failure(self):
        tdSql.query("show dnodes", queryTimes=1)
        if tdSql.queryRows < 3:
            pytest.skip("inner-node failure requires three dnodes")

        database = f"correlated_scalar_fault_{uuid.uuid4().hex[:8]}"
        tdSql.execute(f"create database {database} vgroups 6 replica 1")
        process = None
        failed_node = None
        try:
            tdSql.execute(f"use {database}")
            for index in range(24):
                tdSql.execute(f"create table fault_{index} (ts timestamp, device int, v int)")
            tdSql.query("show vgroups", queryTimes=1)
            owners = {row[0]: row[3] for row in tdSql.queryResult}
            tdSql.query(
                "select table_name, vgroup_id from information_schema.ins_tables "
                f"where db_name = '{database}' and table_name like 'fault_%'",
                queryTimes=1,
            )
            tables = [(name, owners[vgroup]) for name, vgroup in tdSql.queryResult]
            outer = next(name for name, owner in tables if owner == 1)
            inner, inner_owner = next((name, owner) for name, owner in tables if owner != 1)
            tdSql.execute(f"insert into {outer} values ('2026-09-11 00:00:01', 1, 1)")
            tdSql.execute(f"insert into {inner} values ('2026-09-11 00:00:01', 1, 11)")
            sql = (
                "select r.device, (select sleep(30) from "
                f"{database}.{inner} h where h.device = r.device) "
                f"from {database}.{outer} r"
            )
            bin_dir = os.environ.get("TAOS_BIN_PATH") or os.path.join(tdCom.getBuildPath(), "build", "bin")
            process = subprocess.Popen(
                [os.path.join(bin_dir, "taos"), "-c", self.cfg_path, "-h", self.host,
                 "-P", str(self.port), "-s", sql],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            query_id = None
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline and process.poll() is None and query_id is None:
                tdSql.query("select kill_id, sql from performance_schema.perf_queries", queryTimes=1)
                for kill_id, running_sql in tdSql.queryResult:
                    if str(running_sql).strip() == sql:
                        query_id = kill_id
                        break
                if query_id is None:
                    time.sleep(0.1)
            assert query_id is not None, "cross-node correlated query did not become active"

            failed_node = cluster.dnodes[inner_owner - 1]
            failed_node.forcestop()
            failed_node.starttaosd()
            stdout, stderr = process.communicate(timeout=45)
            if process.returncode == 0:
                assert b"1" in stdout, stdout
            else:
                assert stderr, "failed query did not report an error"
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                tdSql.query("select kill_id from performance_schema.perf_queries", queryTimes=1)
                if all(row[0] != query_id for row in tdSql.queryResult):
                    break
                time.sleep(0.2)
            else:
                assert False, "correlated query remained active after inner-node restart"

            tdSql.query(
                f"select r.device, (select max(h.v) from {inner} h "
                f"where h.device = r.device) from {outer} r",
                queryTimes=1,
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 11)
        finally:
            if failed_node is not None and not failed_node.running:
                failed_node.starttaosd()
            if process is not None and process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
            tdSql.execute(f"drop database {database}")
        print("inner node failure releases query ...... [ passed ]")

    def test_correlated_lifecycle(self):
        """Late errors and cancellation release correlated query state

        1. A later offending outer row fails the query; the next call succeeds.
        2. Three independent root queries execute concurrently with correct results.
        3. KILL QUERY and client disconnect interrupt active scalar calls.
        4. After cancellation, the same client can execute the same SQL again.

        Catalog:
            - Query:SubQuery

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-17 Codex Add error recovery, concurrent-root and cancellation checks.

        """
        self.do_lifecycle()

    def test_correlated_inner_node_failure(self):
        """An inner vnode failure terminates or retries an active scalar call

        1. Fail the inner vnode's dnode while a correlated call is active.
        2. The query leaves perf_queries and a fresh call succeeds after restart.

        Catalog:
            - Query:SubQuery

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-17 Codex Add isolated three-dnode inner failure check.

        """
        self.do_inner_node_failure()
