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

import socket
import threading
import time

import taos
from new_test_framework.utils import tdLog, tdSql


class TestRenameColumnConcurrent:
    """Concurrency and cache-refresh behaviour of super-table RENAME COLUMN.

    A rename only changes the display name - colId and the stored data are
    untouched - and translation pins column names to colIds before execution.
    These cases verify the two consequences that matter to users: a query that
    is already running is unaffected by a concurrent rename, and other sessions
    pick up the new name once their catalog cache refreshes.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbname = "db_rename_concurrent"
        cls.rows_per_table = 2000
        cls.num_tables = 10

    def new_conn(self):
        """Open an independent connection so each session has its own catalog cache."""
        return taos.connect(host=socket.gethostname(), port=6030, user="root", password="taosdata")

    def prepare_data(self):
        tdSql.execute(f"drop database if exists {self.dbname}")
        tdSql.execute(f"create database {self.dbname} vgroups 2 keep 36500d")
        tdSql.execute(f"use {self.dbname}")
        tdSql.execute(
            f"create table {self.dbname}.meters "
            f"(ts timestamp, current float, voltage int, phase float) "
            f"tags (location binary(64), groupId int)"
        )
        for i in range(self.num_tables):
            tdSql.execute(
                f"create table {self.dbname}.d{i} using {self.dbname}.meters tags('city{i}', {i})"
            )
        # enough rows that the query in TC-20 is still running when the rename lands
        base = 1780000000000
        for i in range(self.num_tables):
            values = ",".join(
                f"({base + j * 10}, {j % 100}.5, {200 + (j % 50)}, 0.{j % 10})"
                for j in range(self.rows_per_table)
            )
            tdSql.execute(f"insert into {self.dbname}.d{i} values {values}")

    def query_during_rename(self):
        """TC-20: a query already in flight is not disturbed by a concurrent rename.

        Session A runs a large aggregate query while session B renames one of the
        columns it reads. A must return a complete, correct result under the
        column alias it was parsed with - no schema-mismatch error, no short read.
        """
        tdLog.info("========== TC-20: rename while a query is in flight")
        expected_rows = self.num_tables * self.rows_per_table

        result = {}

        def run_query():
            conn = self.new_conn()
            try:
                cur = conn.cursor()
                cur.execute(f"use {self.dbname}")
                # a deliberately heavy query so it is still executing during the rename
                cur.execute(
                    f"select count(*), avg(voltage), sum(current) from {self.dbname}.meters"
                )
                rows = cur.fetchall()
                result["rows"] = rows
                result["desc"] = [d[0] for d in cur.description]
                cur.close()
            except Exception as e:
                result["error"] = e
            finally:
                conn.close()

        t = threading.Thread(target=run_query)
        t.start()
        # give the query time to be parsed and start executing, then rename underneath it
        time.sleep(0.3)
        rename_err = None
        try:
            tdSql.execute(f"alter stable {self.dbname}.meters rename column voltage volt")
        except Exception as e:
            rename_err = e
        t.join(timeout=120)
        assert not t.is_alive(), "in-flight query did not finish within 120s"

        assert rename_err is None, f"rename failed while a query was running: {rename_err}"
        assert "error" not in result, f"in-flight query failed during rename: {result.get('error')}"
        # the aggregate must be complete and correct
        assert result["rows"][0][0] == expected_rows, (
            f"in-flight query returned {result['rows'][0][0]} rows, expected {expected_rows}"
        )
        assert result["rows"][0][1] is not None, "avg(voltage) came back NULL during rename"
        tdLog.info(f"in-flight query result headers: {result['desc']}")

        # the rename did take effect for anything parsed afterwards
        tdSql.query(f"describe {self.dbname}.meters")
        names = [row[0] for row in tdSql.queryResult]
        assert "volt" in names and "voltage" not in names, names

    def cache_refresh_across_sessions(self):
        """TC-22: another session sees the new name once its catalog cache refreshes.

        The rename bumps the schema version, so a session that cached the old
        schema refreshes within a heartbeat period. After that the new name
        resolves and the old one does not.
        """
        tdLog.info("========== TC-22: other sessions pick up the new column name")
        conn = self.new_conn()
        try:
            cur = conn.cursor()
            cur.execute(f"use {self.dbname}")
            # warm this session's catalog cache with the current schema
            cur.execute(f"select volt from {self.dbname}.meters limit 1")
            cur.fetchall()

            # rename from the main session
            tdSql.execute(f"alter stable {self.dbname}.meters rename column volt voltage2")

            # the other session should converge on the new name within a heartbeat window
            deadline = time.time() + 60
            last_err = None
            seen_new = False
            while time.time() < deadline:
                try:
                    cur.execute(f"select voltage2 from {self.dbname}.meters limit 1")
                    cur.fetchall()
                    seen_new = True
                    break
                except Exception as e:
                    last_err = e
                    time.sleep(1)
            assert seen_new, f"session did not see the new column name within 60s: {last_err}"

            # and the old name must no longer resolve in that same session
            old_name_failed = False
            try:
                cur.execute(f"select volt from {self.dbname}.meters limit 1")
                cur.fetchall()
            except Exception:
                old_name_failed = True
            assert old_name_failed, "old column name still resolved after the rename"
            cur.close()
        finally:
            conn.close()

    def concurrent_renames_serialize(self):
        """TC-21: concurrent renames of the same column serialize; exactly one wins.

        The rename runs inside an sdb transaction, so racing sessions cannot both
        apply. Whichever loses must fail cleanly (the source column no longer
        exists) rather than corrupt the schema.
        """
        tdLog.info("========== TC-21: concurrent renames of the same column")
        tdSql.execute(f"create table {self.dbname}.cstb (ts timestamp, cc1 int) tags(ct int)")

        outcomes = []
        lock = threading.Lock()

        def do_rename(target):
            conn = self.new_conn()
            try:
                cur = conn.cursor()
                cur.execute(f"alter stable {self.dbname}.cstb rename column cc1 {target}")
                cur.close()
                with lock:
                    outcomes.append((target, None))
            except Exception as e:
                with lock:
                    outcomes.append((target, e))
            finally:
                conn.close()

        threads = [
            threading.Thread(target=do_rename, args=(name,))
            for name in ("cc1_a", "cc1_b", "cc1_c")
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        assert all(not t.is_alive() for t in threads), "a concurrent rename hung"

        succeeded = [name for name, err in outcomes if err is None]
        assert len(succeeded) == 1, f"expected exactly one rename to win, got {outcomes}"

        # the schema reflects the single winner and nothing else
        tdSql.query(f"describe {self.dbname}.cstb")
        names = [row[0] for row in tdSql.queryResult]
        assert succeeded[0] in names, f"winning name {succeeded[0]} missing from {names}"
        assert "cc1" not in names, f"original column name still present: {names}"
        for name in ("cc1_a", "cc1_b", "cc1_c"):
            if name != succeeded[0]:
                assert name not in names, f"losing rename {name} was applied: {names}"

    def test_rename_column_concurrent(self):
        """MetaData: concurrent RENAME COLUMN and cross-session cache refresh

        1. TC-20: a query in flight is unaffected by a concurrent rename
        2. TC-22: another session sees the new name within a heartbeat period
        3. TC-21: concurrent renames of the same column serialize, one wins

        Catalog:
            - MetaData:Alter

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        History:
            - 2026-08 Created for the stb rename column feature
        """
        self.prepare_data()
        self.query_during_rename()
        self.cache_refresh_across_sessions()
        self.concurrent_renames_serialize()
        tdSql.execute(f"drop database if exists {self.dbname}")
        tdLog.success(f"{__file__} successfully executed")
