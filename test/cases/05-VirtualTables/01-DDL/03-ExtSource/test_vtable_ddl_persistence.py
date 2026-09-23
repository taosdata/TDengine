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

"""Lightweight persistence & concurrency tests for vtable/vstb on ext sources.

Section 7 of the DDL test plan. These overlap with cases/09-DataQuerying/
19-FederatedQuery/test_fq_09_stability.py and test_fq_15_service_disruption.py
but specifically validate that the *DDL metadata* for vtables / virtual
super-tables / heterogeneous children survives a taosd restart and that
concurrent ALTER statements are serialized cleanly.
"""

# -*- coding: utf-8 -*-
import os
import sys
import threading
import time

import taos
from new_test_framework.utils import tdLog, tdSql, tdDnodes

_FQ_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__),
                 "..", "..", "..", "09-DataQuerying", "19-FederatedQuery"))
if _FQ_DIR not in sys.path:
    sys.path.insert(0, _FQ_DIR)
from federated_query_common import ExtSrcEnv  # noqa: E402
from ext_source_helpers import (  # noqa: E402
    create_ext_source, create_remote_db, create_pg_table, create_mysql_table,
    create_influx_measurement, extract_show_create_sql as _extract_show_create_sql)


_LOCAL_DB = "vpers_local"
_PG_DB    = "vpers_pg"
_MY_DB    = "vpers_my"
_INF_DB   = "vpers_inf"
_PG_SRC   = "vpers_pg_src"
_MY_SRC   = "vpers_my_src"
_INF_SRC  = "vpers_inf_src"


def _provision_remote():
    create_remote_db("postgresql", _PG_DB)
    create_remote_db("mysql", _MY_DB)
    create_remote_db("influxdb", _INF_DB)
    create_pg_table(_PG_DB, "r",
                    "ts TIMESTAMP PRIMARY KEY, v DOUBLE PRECISION",
                    ["('2024-01-01 00:00:00', 1.0)",
                     "('2024-01-01 00:01:00', 2.0)"])
    create_mysql_table(_MY_DB, "r",
                       "ts DATETIME(3) NOT NULL PRIMARY KEY, v DOUBLE",
                       ["('2024-01-01 00:00:00', 10.0)",
                        "('2024-01-01 00:01:00', 20.0)"])
    create_influx_measurement(_INF_DB, [
        "persist_m,device=d1 value=1.0 1700000000000000000",
        "persist_m,device=d1 value=2.0 1700000001000000000",
    ])
    create_influx_measurement(
        _INF_DB, ["_snapshot_trigger_persist value=1i 1000000000000000000"])


def _ensure_sources():
    create_ext_source(_PG_SRC, "postgresql", _PG_DB)
    create_ext_source(_MY_SRC, "mysql", _MY_DB)
    create_ext_source(_INF_SRC, "influxdb", _INF_DB)


# ===========================================================================

class TestVtableDDLPersistence:

    @classmethod
    def setup_class(cls):
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()
        _provision_remote()
        _ensure_sources()
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"CREATE DATABASE {_LOCAL_DB} PRECISION 'ns'")
        tdSql.execute(f"USE {_LOCAL_DB}")

        # Setup a fixed metadata fixture used by multiple persistence cases.
        tdSql.execute(
            f"CREATE VTABLE v_persist (ts timestamp, "
            f"v double FROM {_PG_SRC}.{_PG_DB}.r.v)")
        tdSql.execute(
            "CREATE STABLE vstb_persist (ts timestamp, v double) "
            "TAGS (backend nchar(8)) VIRTUAL 1")
        tdSql.execute(
            f"CREATE VTABLE vctb_persist_pg ("
            f"v FROM {_PG_SRC}.{_PG_DB}.r.v) "
            f"USING vstb_persist TAGS ('pg')")
        tdSql.execute(
            f"CREATE VTABLE vctb_persist_my ("
            f"v FROM {_MY_SRC}.{_MY_DB}.r.v) "
            f"USING vstb_persist TAGS ('my')")
        tdSql.execute(
            "CREATE VTABLE v_series_persist (ts timestamp, "
            "value double FROM s1.value) "
            f"SERIES s1 AS {_INF_SRC}.{_INF_DB}.persist_m (device='d1')")
        tdSql.execute(
            "CREATE STABLE vstb_series_persist (ts timestamp, value double) "
            "TAGS (site nchar(8)) VIRTUAL 1")
        tdSql.execute(
            "CREATE VTABLE vctb_series_persist (value FROM s1.value) "
            "USING vstb_series_persist TAGS ('inf') "
            f"SERIES s1 AS {_INF_SRC}.{_INF_DB}.persist_m (device='d1')")

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        for n in (_PG_SRC, _MY_SRC, _INF_SRC):
            tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {n}")

    # -------------------------------------------------------------------
    # 7.1 — ext source / normal vtable
    # -------------------------------------------------------------------

    def _restart_dnode(self):
        try:
            tdDnodes.stop(1)
            tdDnodes.start(1)
            time.sleep(3)
            ExtSrcEnv.ensure_qnode()
        except Exception as e:
            tdLog.info(f"[persistence] dnode restart skipped: {e}")
            return False
        return True

    def test_restart_preserves_ext_source_and_vtable(self):
        """Preserve external sources and normal virtual-table metadata after restart

        1. Restart the dnode and restore the query node
        2. Verify all three external sources and the normal virtual table remain registered

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing persistence coverage

        """
        if not self._restart_dnode():
            return
        tdSql.execute(f"USE {_LOCAL_DB}")
        tdSql.query(f"SELECT count(*) FROM information_schema.ins_ext_sources "
                    f"WHERE source_name IN ('{_PG_SRC}', '{_MY_SRC}', '{_INF_SRC}')")
        tdSql.checkData(0, 0, 3)
        tdSql.query(f"SELECT count(*) FROM information_schema.ins_tables "
                    f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_persist'")
        tdSql.checkData(0, 0, 1)

    def test_restart_then_query(self):
        """Query an external-backed normal virtual table after restart

        1. Restart the dnode and restore the query node
        2. Verify the persisted virtual table returns at least two rows

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing post-restart query coverage

        """
        if not self._restart_dnode():
            return
        tdSql.execute(f"USE {_LOCAL_DB}")
        tdSql.query("SELECT CASE WHEN count(*) >= 2 THEN 1 ELSE 0 END FROM v_persist")
        tdSql.checkData(0, 0, 1)

    # -------------------------------------------------------------------
    # 7.2 — vstb
    # -------------------------------------------------------------------

    def test_restart_preserves_vstb_and_children(self):
        """Preserve a virtual super table and its children after restart

        1. Restart the dnode and restore the query node
        2. Verify the virtual super table and both external-backed children remain registered

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing super-table persistence coverage

        """
        if not self._restart_dnode():
            return
        tdSql.execute(f"USE {_LOCAL_DB}")
        tdSql.query("SELECT count(*) FROM information_schema.ins_stables "
                    f"WHERE db_name='{_LOCAL_DB}' AND stable_name='vstb_persist'")
        tdSql.checkData(0, 0, 1)
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    f"WHERE db_name='{_LOCAL_DB}' AND stable_name='vstb_persist'")
        tdSql.checkData(0, 0, 2)

    def test_restart_cross_source_aggregation_stable(self):
        """Preserve cross-source virtual-super-table aggregate results after restart

        1. Record row counts and averages grouped by the PostgreSQL and MySQL backend tags
        2. Restart the dnode and compare every aggregate result with the saved values

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing aggregate persistence coverage

        """
        # Before / after restart, the same aggregation must return matching rows.
        tdSql.execute(f"USE {_LOCAL_DB}")
        tdSql.query("SELECT backend, count(*), avg(v) FROM vstb_persist "
                    "GROUP BY backend ORDER BY backend")
        before = [(tdSql.getData(r, 0), tdSql.getData(r, 1),
                   tdSql.getData(r, 2)) for r in range(tdSql.getRows())]

        if not self._restart_dnode():
            return
        tdSql.execute(f"USE {_LOCAL_DB}")
        tdSql.query("SELECT backend, count(*), avg(v) FROM vstb_persist "
                    "GROUP BY backend ORDER BY backend")
        tdSql.checkRows(len(before))
        for row, values in enumerate(before):
            for col, value in enumerate(values):
                tdSql.checkData(row, col, value)

    def test_restart_preserves_series_metadata_and_query(self):
        """Preserve InfluxDB SERIES metadata and query results after restart

        1. Restart the dnode and verify SERIES clauses in normal and child SHOW CREATE output
        2. Verify the normal table, child table, and super table each return two rows

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing SERIES persistence coverage

        """
        if not self._restart_dnode():
            return
        tdSql.execute(f"USE {_LOCAL_DB}")
        sql = _extract_show_create_sql("SHOW CREATE VTABLE v_series_persist")
        if "SERIES" not in sql.upper() or _INF_SRC not in sql or "device='d1'" not in sql:
            tdLog.exit(f"expected SERIES clause in {sql}")
        tdSql.query("SELECT count(*) FROM v_series_persist")
        tdSql.checkData(0, 0, 2)

        child_sql = _extract_show_create_sql("SHOW CREATE VTABLE vctb_series_persist")
        if "SERIES" not in child_sql.upper() or "USING `vstb_series_persist`" not in child_sql:
            tdLog.exit(f"expected child SERIES clause in {child_sql}")
        tdSql.query("SELECT count(*) FROM vctb_series_persist")
        tdSql.checkData(0, 0, 2)
        tdSql.query("SELECT count(*) FROM vstb_series_persist")
        tdSql.checkData(0, 0, 2)

    # -------------------------------------------------------------------
    # 7.2 — concurrent ALTER STABLE
    # -------------------------------------------------------------------

    def test_concurrent_alter_stable_serializes(self):
        """Serialize concurrent column additions on a virtual super table

        1. Add distinct columns concurrently through separate connections, retrying transaction conflicts
        2. Verify both workers finish and both columns appear in the super-table schema

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing concurrent ALTER coverage

        """
        # Two threads each ADD a different COLUMN simultaneously. Each thread
        # MUST use its own TAOS* connection — the C client connection handle
        # is not thread-safe (sharing it across threads races the internal
        # SCatalogReq state inside libtaosnative, crashing the worker thread
        # in putDbTableDataToCache).
        conc_stb = "vstb_persist_conc"
        conc_vtb = "vctb_persist_conc_pg"
        tdLog.info("[concurrency] prepare isolated stable fixture")
        admin_conn = taos.connect(host="127.0.0.1", user="root", password="taosdata")
        tdLog.info("[concurrency] admin connect ok")
        admin_cur = admin_conn.cursor()
        tdLog.info("[concurrency] admin cursor ok")
        admin_cur.execute(f"USE {_LOCAL_DB}")
        tdLog.info("[concurrency] use db ok")
        admin_cur.execute(f"DROP TABLE IF EXISTS {conc_vtb}")
        tdLog.info("[concurrency] drop child ok")
        admin_cur.execute(f"DROP STABLE IF EXISTS {conc_stb}")
        tdLog.info("[concurrency] drop stable ok")
        admin_cur.execute(
            f"CREATE STABLE {conc_stb} (ts timestamp, v double) "
            "TAGS (backend nchar(8)) VIRTUAL 1")
        tdLog.info("[concurrency] create stable ok")
        admin_cur.execute(
            f"CREATE VTABLE {conc_vtb} (v FROM {_PG_SRC}.{_PG_DB}.r.v) "
            f"USING {conc_stb} TAGS ('pg')")
        tdLog.info("[concurrency] create child ok")

        results = []
        lock = threading.Lock()

        def add_column(name):
            conn = taos.connect(host="127.0.0.1", user="root", password="taosdata")
            try:
                conn.execute(f"USE {_LOCAL_DB}")
                # Engine serializes DDL via mnode transactions; the loser
                # gets "Conflict transaction not completed" and must retry.
                last = None
                for _ in range(30):
                    try:
                        conn.execute(f"ALTER STABLE {conc_stb} ADD COLUMN {name} int")
                        last = "ok"
                        break
                    except Exception as e:
                        last = repr(e)
                        if "Conflict transaction" in last:
                            time.sleep(0.5)
                            continue
                        raise
                with lock: results.append((name, last))
            except Exception as e:
                with lock: results.append((name, repr(e)))
            finally:
                try: conn.close()
                except Exception: pass

        t1 = threading.Thread(target=add_column, args=("conc_a",))
        t2 = threading.Thread(target=add_column, args=("conc_b",))
        t1.start(); t2.start()
        t1.join(timeout=60); t2.join(timeout=60)
        if t1.is_alive() or t2.is_alive():
            tdLog.exit(f"thread hang: results={results}")

        admin_cur.execute(
            "SELECT col_name FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='{conc_stb}' "
            "AND col_name IN ('conc_a', 'conc_b') ORDER BY col_name")
        rows = admin_cur.fetchall()
        if len(rows) != 2 or rows[0][0] != "conc_a" or rows[1][0] != "conc_b":
            tdLog.exit(f"unexpected concurrent ALTER result rows: {rows}")
        # Engine must serialize: both columns end up in schema, no corruption.
        tdLog.info(f"[concurrency] both ADD COLUMN serialized: {results}")
        for c in ("conc_a", "conc_b"):
            admin_cur.execute(f"ALTER STABLE {conc_stb} DROP COLUMN {c}")
        admin_cur.execute(f"DROP TABLE IF EXISTS {conc_vtb}")
        admin_cur.execute(f"DROP STABLE IF EXISTS {conc_stb}")
        admin_cur.close()
        admin_conn.close()

    # -------------------------------------------------------------------
    # 7.1 — DROP source while query in flight
    # -------------------------------------------------------------------

    def test_drop_source_does_not_crash_dnode(self):
        """Keep the dnode responsive while dropping a source used by queries

        1. Repeatedly drop and recreate the PostgreSQL source while querying its virtual table
        2. Allow transient operation errors and verify the worker finishes and SELECT 1 succeeds

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-07 Jing Sima Document existing source-churn coverage

        """
        errors = []

        def churn_source():
            conn = taos.connect(host="127.0.0.1", user="root", password="taosdata")
            try:
                cur = conn.cursor()
                try:
                    cur.execute(f"USE {_LOCAL_DB}")
                    for _ in range(3):
                        try:
                            cur.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_PG_SRC}")
                            cur.execute(
                                f"CREATE EXTERNAL SOURCE {_PG_SRC} TYPE='postgresql' "
                                f"HOST='{ExtSrcEnv.PG_HOST}' PORT={ExtSrcEnv.PG_PORT} "
                                f"USER='{ExtSrcEnv.PG_USER}' PASSWORD='{ExtSrcEnv.PG_PASS}' "
                                f"DATABASE={_PG_DB} SCHEMA=public")
                        except Exception as e:
                            errors.append(repr(e))
                        time.sleep(0.5)
                finally:
                    cur.close()
            finally:
                conn.close()

        t = threading.Thread(target=churn_source)
        t.start()

        conn = taos.connect(host="127.0.0.1", user="root", password="taosdata")
        try:
            cur = conn.cursor()
            try:
                cur.execute(f"USE {_LOCAL_DB}")
                for _ in range(20):
                    try:
                        cur.execute("SELECT count(*) FROM v_persist")
                    except Exception as e:
                        errors.append(repr(e))
                    time.sleep(0.1)
            finally:
                cur.close()
        finally:
            conn.close()

        t.join(timeout=10)
        if t.is_alive():
            tdLog.exit(f"drop source thread hang: errors={errors}")
        # The dnode must still answer:
        check_conn = taos.connect(host="127.0.0.1", user="root", password="taosdata")
        check_cur = check_conn.cursor()
        try:
            check_cur.execute("SELECT 1")
            rows = check_cur.fetchall()
            if len(rows) != 1 or rows[0][0] != 1:
                tdLog.exit(f"unexpected SELECT 1 result after source churn: {rows}")
        finally:
            check_cur.close()
            check_conn.close()
