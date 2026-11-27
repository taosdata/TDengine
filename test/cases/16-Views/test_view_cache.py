# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import json
import os
import queue
import subprocess
import sys
import textwrap
import threading
import time

from new_test_framework.utils import tdLog, tdSql


# A long-lived client process. Being a separate process is essential: the view and
# table caches are per client process, so DDL executed by the pytest connection must
# not evict the caches of this one.
_LONG_CLIENT = textwrap.dedent(
    """
    import json, sys
    import taos

    conn = taos.connect(config=sys.argv[1])
    cur = conn.cursor()
    for line in sys.stdin:
        sql = line.rstrip("\\n")
        if not sql:
            continue
        try:
            cur.execute(sql)
            rows = [[str(c) for c in row] for row in cur.fetchall()]
            print(json.dumps({"ok": True, "rows": rows}), flush=True)
        except Exception as e:
            print(json.dumps({"ok": False, "errno": getattr(e, "errno", None), "msg": str(e)}), flush=True)
    """
)


class LongClient:
    """A separate client process that keeps its catalog cache alive across queries."""

    def __init__(self, cfg_dir):
        self._q = queue.Queue()
        self._p = subprocess.Popen(
            [sys.executable, "-c", _LONG_CLIENT, cfg_dir],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
        threading.Thread(target=self._reader, daemon=True).start()

    def _reader(self):
        for line in self._p.stdout:
            line = line.strip()
            if line:
                self._q.put(line)

    def execute(self, sql, timeout=60):
        self._p.stdin.write(sql + "\n")
        self._p.stdin.flush()
        try:
            return json.loads(self._q.get(timeout=timeout))
        except queue.Empty:
            raise AssertionError(f"long-lived client timed out executing: {sql}")

    def query_first_value(self, sql, timeout=60):
        res = self.execute(sql, timeout)
        if not res.get("ok"):
            return None, res
        rows = res.get("rows") or []
        if not rows:
            return None, res
        return int(float(rows[0][0])), res

    def close(self):
        try:
            self._p.stdin.close()
        except Exception:
            pass
        try:
            self._p.wait(timeout=10)
        except Exception:
            self._p.kill()


class TestViewCache:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @classmethod
    def teardown_class(cls):
        # The framework may reuse the cluster across test classes and
        # information_schema.ins_views is cluster-wide. Drop the databases created
        # here so later cases see a clean view catalog.
        names = [f"db_view_cache{i}" for i in range(1, 13)]
        for name in names:
            try:
                tdSql.cursor.execute(f"drop database if exists {name}")
            except Exception as e:
                tdLog.notice(f"teardown: failed to drop {name}: {e}")

    # ------------------------------------------------------------------ helpers

    def _client_cfg_dir(self):
        # The pytest process connects with the psim client config; self.cfg_path is the
        # dnode config dir. The long-lived helper must use the same psim config as the
        # pytest connection.
        candidate = os.path.join(self.work_dir, "psim", "cfg")
        if os.path.isdir(candidate):
            return candidate
        return self.cfg_path

    def _long_client(self):
        return LongClient(self._client_cfg_dir())

    def _wait_value(self, cli, sql, expected, timeout=60):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            last, res = cli.query_first_value(sql)
            if last == expected:
                return
            time.sleep(1)
        raise AssertionError(f"value did not converge to {expected}, last:{last}, res:{res}")

    def _prepare_view_db(self, dbname, tables):
        tdSql.prepare(dbname=dbname, drop=True, vgroups=2)
        for name, value in tables:
            tdSql.execute(f"create table {name} (ts timestamp, v int)")
            tdSql.execute(f"insert into {name} values (1700000000000, {value})")

    # ------------------------------------------------- single client, scenarios

    def test_single_client_scenario1_source_table_recreated(self):
        """Original scenario 1: source table dropped and recreated with the same name

        The view still points to the old table UID cached in the client catalog.
        After the source table is dropped and recreated, querying through the view
        must resolve the new table by name instead of returning 0x2603.

        Catalog:
            - View

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-11-17 wpan Added view cache regression coverage
        """
        self._prepare_view_db("db_view_cache1", [("t_r", 11)])
        tdSql.execute("create view v_r as select ts, v from t_r")
        tdSql.query("select v from v_r")
        tdSql.checkData(0, 0, 11)

        tdSql.execute("drop table t_r")
        tdSql.execute("create table t_r (ts timestamp, v int)")
        tdSql.execute("insert into t_r values (1700000000000, 55)")

        # Immediate query after the DDL: must see the recreated table, not 0x2603.
        tdSql.query("select v from v_r")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 55)

    def test_single_client_scenario2_switch_a_to_b(self):
        """Original scenario 2: view switched from a to b, table a is kept

        The client catalog still cached the old view definition on a. After
        CREATE OR REPLACE VIEW the next query in the same connection must read b.

        Catalog:
            - View

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-11-17 wpan Added view cache regression coverage
        """
        self._prepare_view_db("db_view_cache2", [("t_a", 11), ("t_b", 22)])
        tdSql.execute("create view v_s2 as select ts, v from t_a")
        tdSql.query("select v from v_s2")
        tdSql.checkData(0, 0, 11)

        tdSql.execute("create or replace view v_s2 as select ts, v from t_b")
        tdSql.query("select v from v_s2")
        tdSql.checkData(0, 0, 22)

        # The retained source table is untouched.
        tdSql.query("select v from t_a")
        tdSql.checkData(0, 0, 11)

    def test_single_client_scenario3_switch_b_to_c(self):
        """Original scenario 3: view switched from b to c, table b is kept

        Same stale-definition path as scenario 2 but starting from b; the query
        right after the second switch must read c.

        Catalog:
            - View

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-11-17 wpan Added view cache regression coverage
        """
        self._prepare_view_db("db_view_cache3", [("t_b", 22), ("t_c", 33)])
        tdSql.execute("create view v_s3 as select ts, v from t_b")
        tdSql.query("select v from v_s3")
        tdSql.checkData(0, 0, 22)

        tdSql.execute("create or replace view v_s3 as select ts, v from t_c")
        tdSql.query("select v from v_s3")
        tdSql.checkData(0, 0, 33)

        tdSql.query("select v from t_b")
        tdSql.checkData(0, 0, 22)

    def test_single_client_scenario4_new_table_cutover(self):
        """Original scenario 4: new table built and loaded, view switched to it

        The old table is kept for rollback. The query right after the switch must
        read the new table, and the old table must stay intact for rollback.

        Catalog:
            - View

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-11-17 wpan Added view cache regression coverage
        """
        self._prepare_view_db("db_view_cache4", [("t_old", 11)])
        tdSql.execute("create view v_s4 as select ts, v from t_old")
        tdSql.query("select v from v_s4")
        tdSql.checkData(0, 0, 11)

        tdSql.execute("create table t_new (ts timestamp, v int)")
        tdSql.execute("insert into t_new values (1700000000000, 44)")
        tdSql.execute("create or replace view v_s4 as select ts, v from t_new")
        tdSql.query("select v from v_s4")
        tdSql.checkData(0, 0, 44)

        # Rollback table still queryable.
        tdSql.query("select v from t_old")
        tdSql.checkData(0, 0, 11)

    # ---------------------------------------- single client, supporting cases

    def test_single_client_create_drop_view(self):
        """CREATE/DROP VIEW responses keep the local catalog in sync

        Not one of the four original scenarios: covers the create/drop response
        handling of this fix. CREATE VIEW installs the returned metadata and
        DROP VIEW evicts the entry, so the next statement in the same connection
        is correct and a dropped view returns 0x2603 immediately.

        Catalog:
            - View

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-11-17 wpan Added view cache regression coverage
        """
        self._prepare_view_db("db_view_cache5", [("t_src", 11)])

        tdSql.execute("create view v_c as select ts, v from t_src")
        tdSql.query("select v from v_c")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 11)

        tdSql.execute("drop view v_c")
        tdSql.error("select v from v_c", expectedErrno=0x2603)

        tdSql.execute("create view v_c as select ts, v from t_src")
        tdSql.query("select v from v_c")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 11)

    def test_single_client_drop_database_with_views(self):
        """Dropping a database with cached views keeps the client usable

        Not one of the four original scenarios: guards the view rent cleanup path
        touched by this fix (removing a view entry with no view cache present).
        After dropping a database that holds cached views the client must stay
        usable and re-creating the same database must work.

        Catalog:
            - View

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-11-17 wpan Added view cache regression coverage
        """
        self._prepare_view_db("db_view_cache6", [("t_x", 11)])
        tdSql.execute("create view v_x1 as select ts, v from t_x")
        tdSql.execute("create view v_x2 as select ts, v from t_x")
        tdSql.query("select v from v_x1")
        tdSql.checkData(0, 0, 11)
        tdSql.query("select v from v_x2")
        tdSql.checkData(0, 0, 11)

        tdSql.execute("drop database db_view_cache6")
        tdSql.query("show databases")
        assert tdSql.checkRowsV2(1, operator=">="), "client unusable after dropping a database with views"

        self._prepare_view_db("db_view_cache6", [("t_x", 66)])
        tdSql.execute("create view v_x1 as select ts, v from t_x")
        tdSql.query("select v from v_x1")
        tdSql.checkData(0, 0, 66)

    # -------------------------------------------------- two client, scenarios

    def test_two_clients_scenario1_source_table_recreated(self):
        """Original scenario 1 with two clients: A recreates the source table

        B keeps the old table UID in its catalog. The failure triggered by the
        recreated table must be recovered by re-resolving the source table by
        name; B's very next query has to return the new data, not 0x2603.

        Catalog:
            - View

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-11-17 wpan Added view cache regression coverage
        """
        self._prepare_view_db("db_view_cache7", [("t_s", 11)])
        tdSql.execute("create view v_s as select ts, v from t_s")

        cli = self._long_client()
        try:
            val, res = cli.query_first_value("select v from db_view_cache7.v_s")
            assert val == 11, f"unexpected initial value, res:{res}"

            # Client A drops and recreates the source table (new UID).
            tdSql.execute("drop table t_s")
            tdSql.execute("create table t_s (ts timestamp, v int)")
            tdSql.execute("insert into t_s values (1700000000000, 77)")

            val, res = cli.query_first_value("select v from db_view_cache7.v_s")
            assert val == 77, f"view did not re-resolve the recreated source table, res:{res}"
            val, res = cli.query_first_value("select v from db_view_cache7.v_s")
            assert val == 77, f"second query after recovery failed, res:{res}"
        finally:
            cli.close()

    def test_two_clients_scenario2_switch_a_to_b(self):
        """Original scenario 2 with two clients: A switches the view a -> b

        B cached the definition on a and only learns the change from the
        heartbeat; it must converge on b, not keep returning a's rows.

        Catalog:
            - View

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-11-17 wpan Added view cache regression coverage
        """
        self._prepare_view_db("db_view_cache8", [("t_a", 11), ("t_b", 22)])
        tdSql.execute("create view v_s2 as select ts, v from t_a")

        cli = self._long_client()
        try:
            val, res = cli.query_first_value("select v from db_view_cache8.v_s2")
            assert val == 11, f"unexpected initial value, res:{res}"

            tdSql.execute("create or replace view v_s2 as select ts, v from t_b")
            self._wait_value(cli, "select v from db_view_cache8.v_s2", 22)
        finally:
            cli.close()

    def test_two_clients_scenario3_switch_b_to_c(self):
        """Original scenario 3 with two clients: A switches the view b -> c

        B cached the definition on b; the heartbeat refresh must move it to c.

        Catalog:
            - View

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-11-17 wpan Added view cache regression coverage
        """
        self._prepare_view_db("db_view_cache9", [("t_b", 22), ("t_c", 33)])
        tdSql.execute("create view v_s3 as select ts, v from t_b")

        cli = self._long_client()
        try:
            val, res = cli.query_first_value("select v from db_view_cache9.v_s3")
            assert val == 22, f"unexpected initial value, res:{res}"

            tdSql.execute("create or replace view v_s3 as select ts, v from t_c")
            self._wait_value(cli, "select v from db_view_cache9.v_s3", 33)
        finally:
            cli.close()

    def test_two_clients_scenario4_new_table_cutover(self):
        """Original scenario 4 with two clients: A cuts the view over to a new table

        A builds and loads a new table, then switches the view; the old table is
        kept for rollback. B must converge on the new table and the old table
        must still be queryable on A.

        Catalog:
            - View

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-11-17 wpan Added view cache regression coverage
        """
        self._prepare_view_db("db_view_cache10", [("t_old", 11)])
        tdSql.execute("create view v_s4 as select ts, v from t_old")

        cli = self._long_client()
        try:
            val, res = cli.query_first_value("select v from db_view_cache10.v_s4")
            assert val == 11, f"unexpected initial value, res:{res}"

            tdSql.execute("create table t_new (ts timestamp, v int)")
            tdSql.execute("insert into t_new values (1700000000000, 44)")
            tdSql.execute("create or replace view v_s4 as select ts, v from t_new")
            self._wait_value(cli, "select v from db_view_cache10.v_s4", 44)

            tdSql.query("select v from t_old")
            tdSql.checkData(0, 0, 11)
        finally:
            cli.close()

    # ------------------------------------------- two client, supporting cases

    def test_two_clients_view_drop_recreate(self):
        """A drops and recreates the view, B converges via heartbeat

        Not one of the four original scenarios: a recreated view has a new
        viewId, so the heartbeat push must remove the old instance from B's
        catalog and install the new definition.

        Catalog:
            - View

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-11-17 wpan Added view cache regression coverage
        """
        self._prepare_view_db("db_view_cache11", [("t_a", 11), ("t_b", 22)])
        tdSql.execute("create view v_d as select ts, v from t_a")

        cli = self._long_client()
        try:
            val, res = cli.query_first_value("select v from db_view_cache11.v_d")
            assert val == 11, f"unexpected initial value, res:{res}"

            tdSql.execute("drop view v_d")
            tdSql.execute("create view v_d as select ts, v from t_b")
            self._wait_value(cli, "select v from db_view_cache11.v_d", 22)
        finally:
            cli.close()

    def test_two_clients_many_views_batching(self):
        """Heartbeat validates every view of a multi-view replacement

        Not one of the four original scenarios: A replaces 25 views, more than
        one rent slot holds. B must converge on every one of them; the batched
        heartbeat validation collects further rent slots within a round and only
        commits after all slots are verified, so no view keeps its old data.

        Catalog:
            - View

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-11-17 wpan Added view cache regression coverage
        """
        view_num = 25
        self._prepare_view_db("db_view_cache12", [("t_a", 11), ("t_b", 22)])
        for i in range(view_num):
            tdSql.execute(f"create view v_m{i} as select ts, v from t_a")

        cli = self._long_client()
        try:
            for i in range(view_num):
                val, res = cli.query_first_value(f"select v from db_view_cache12.v_m{i}")
                assert val == 11, f"view v_m{i} initial value mismatch, res:{res}"

            for i in range(view_num):
                tdSql.execute(f"create or replace view v_m{i} as select ts, v from t_b")

            pending = set(range(view_num))
            deadline = time.time() + 90
            while pending and time.time() < deadline:
                time.sleep(1)
                for i in list(pending):
                    val, res = cli.query_first_value(f"select v from db_view_cache12.v_m{i}")
                    if val == 22:
                        pending.discard(i)
            assert not pending, f"views were not refreshed by heartbeat: {sorted(pending)}"
        finally:
            cli.close()
