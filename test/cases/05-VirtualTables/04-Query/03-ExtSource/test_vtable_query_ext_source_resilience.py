###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

"""Runtime resilience checks for virtual tables backed by external sources."""

# -*- coding: utf-8 -*-
import os
import sys

from new_test_framework.utils import tdLog, tdSql

_DDL_EXT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__),
                 "..", "..", "01-DDL", "03-ExtSource"))
if _DDL_EXT_DIR not in sys.path:
    sys.path.insert(0, _DDL_EXT_DIR)

from ext_source_helpers import (  # noqa: E402
    ExtSrcEnv,
    create_ext_source,
    create_influx_measurement,
    create_pg_table,
    create_remote_db,
)

_LOCAL_DB = "vq_resilience_local"
_PG_DB = "vq_resilience_pg"
_PG_SRC = "vq_resilience_pg_src"
_INF_DB = "vq_resilience_inf"
_INF_SRC = "vq_resilience_inf_src"
_T0 = 1700000000000000000


def _pg_ver():
    return ExtSrcEnv.PG_VERSIONS[0]


def _influx_ver():
    return ExtSrcEnv.INFLUX_VERSIONS[0]


def _check_count(sql, expected):
    tdSql.query(sql)
    tdSql.checkData(0, 0, expected)


def _prepare_remote_data():
    create_remote_db("postgresql", _PG_DB)
    create_pg_table(
        _PG_DB, "r_pg",
        "ts TIMESTAMP PRIMARY KEY, v INTEGER",
        [
            "('2024-01-01 00:00:00', 11)",
            "('2024-01-01 00:00:01', 12)",
        ])
    create_pg_table(
        _PG_DB, "r_pg_child",
        "ts TIMESTAMP PRIMARY KEY, v BIGINT",
        [
            "('2024-01-01 00:00:00', 101)",
            "('2024-01-01 00:00:01', 102)",
        ])

    create_remote_db("influxdb", _INF_DB)
    create_influx_measurement(_INF_DB, [
        f"r_inf,site=normal v=21i {_T0}",
        f"r_inf,site=normal v=22i {_T0 + 1000000000}",
        f"r_inf_child,site=child v=201i {_T0}",
        f"r_inf_child,site=child v=202i {_T0 + 1000000000}",
    ])
    create_influx_measurement(
        _INF_DB, ["_snapshot_trigger_ value=1i 1000000000000000000"])


class TestVtableQueryExtSourceResilience:

    @classmethod
    def setup_class(cls):
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()
        _prepare_remote_data()
        create_ext_source(_PG_SRC, "postgresql", _PG_DB)
        create_ext_source(_INF_SRC, "influxdb", _INF_DB)

        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"CREATE DATABASE {_LOCAL_DB} PRECISION 'ms'")
        tdSql.execute(f"USE {_LOCAL_DB}")
        tdSql.execute(
            f"CREATE VTABLE v_pg_normal (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.r_pg.v)")
        tdSql.execute(
            f"CREATE VTABLE v_inf_normal (ts timestamp, "
            f"v bigint FROM {_INF_SRC}.{_INF_DB}.r_inf.v)")
        tdSql.execute(
            "CREATE STABLE vstb_runtime (ts timestamp, v bigint) "
            "TAGS (backend nchar(16)) VIRTUAL 1")
        tdSql.execute(
            f"CREATE VTABLE vctb_pg ("
            f"v FROM {_PG_SRC}.{_PG_DB}.r_pg_child.v) "
            "USING vstb_runtime TAGS ('pg')")
        tdSql.execute(
            f"CREATE VTABLE vctb_inf ("
            f"v FROM {_INF_SRC}.{_INF_DB}.r_inf_child.v) "
            "USING vstb_runtime TAGS ('influx')")

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_PG_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_INF_SRC}")

    def setup_method(self, method):
        tdSql.execute(f"USE {_LOCAL_DB}")

    def test_pg_normal_vtable_query_fails_when_backend_down_and_recovers(self):
        """PostgreSQL-backed normal vtable query fails while backend is down and recovers after restart."""
        _check_count("SELECT count(*) FROM v_pg_normal", 2)
        ExtSrcEnv.stop_pg_instance(_pg_ver())
        try:
            tdSql.error("SELECT count(*) FROM v_pg_normal")
            tdSql.query("DESCRIBE v_pg_normal")
            tdSql.checkData(1, 4, f"{_PG_SRC}.{_PG_DB}.r_pg.v")
        finally:
            ExtSrcEnv.start_pg_instance(_pg_ver())
        _check_count("SELECT count(*) FROM v_pg_normal", 2)

    def test_influx_normal_vtable_query_fails_when_backend_down_and_recovers(self):
        """InfluxDB-backed normal vtable query fails while backend is down and recovers after restart."""
        _check_count("SELECT count(*) FROM v_inf_normal", 2)
        ExtSrcEnv.stop_influx_instance(_influx_ver())
        try:
            tdSql.error("SELECT count(*) FROM v_inf_normal")
            tdSql.query("DESCRIBE v_inf_normal")
            tdSql.checkData(1, 4, f"{_INF_SRC}.{_INF_DB}.r_inf.v")
        finally:
            ExtSrcEnv.start_influx_instance(_influx_ver())
        _check_count("SELECT count(*) FROM v_inf_normal", 2)

    def test_vstable_query_fails_when_one_child_backend_down_and_recovers(self):
        """Virtual stable query fails if one child backend is down; it must not return partial rows."""
        _check_count("SELECT count(*) FROM vstb_runtime", 4)
        _check_count("SELECT count(*) FROM vctb_inf", 2)

        ExtSrcEnv.stop_pg_instance(_pg_ver())
        try:
            tdSql.error("SELECT count(*) FROM vstb_runtime")
            _check_count("SELECT count(*) FROM vctb_inf", 2)
            tdSql.query("DESCRIBE vstb_runtime")
            tdSql.checkData(1, 0, "v")
        finally:
            ExtSrcEnv.start_pg_instance(_pg_ver())

        _check_count("SELECT count(*) FROM vstb_runtime", 4)
