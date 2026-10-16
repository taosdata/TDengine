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
    create_sqlserver_table,
)

_LOCAL_DB = "vq_resilience_local"
_PG_DB = "vq_resilience_pg"
_PG_SRC = "vq_resilience_pg_src"
_INF_DB = "vq_resilience_inf"
_INF_SRC = "vq_resilience_inf_src"
_SS_DB = "vq_resilience_ss"
_SS_SRC = "vq_resilience_ss_src"
_T0 = 1700000000000000000


def _pg_ver():
    return ExtSrcEnv.PG_VERSIONS[0]


def _influx_ver():
    return ExtSrcEnv.INFLUX_VERSIONS[0]


def _sqlserver_ver():
    return ExtSrcEnv.SQLSERVER_VERSIONS[0]


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

    create_remote_db("sqlserver", _SS_DB)
    create_sqlserver_table(
        _SS_DB, "r_ss",
        "ts datetime2(3) NOT NULL PRIMARY KEY, v INT",
        [
            "('2024-01-01 00:00:00.000', 31)",
            "('2024-01-01 00:00:01.000', 32)",
        ])
    create_sqlserver_table(
        _SS_DB, "r_ss_child",
        "ts datetime2(3) NOT NULL PRIMARY KEY, v BIGINT",
        [
            "('2024-01-01 00:00:00.000', 301)",
            "('2024-01-01 00:00:01.000', 302)",
        ])


class TestVtableQueryExtSourceResilience:

    @classmethod
    def setup_class(cls):
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()
        _prepare_remote_data()
        create_ext_source(_PG_SRC, "postgresql", _PG_DB)
        create_ext_source(_INF_SRC, "influxdb", _INF_DB)
        create_ext_source(_SS_SRC, "sqlserver", _SS_DB)

        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"CREATE DATABASE {_LOCAL_DB} PRECISION 'ns'")
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
        tdSql.execute(
            f"CREATE VTABLE v_ss_normal (ts timestamp, "
            f"v int FROM {_SS_SRC}.{_SS_DB}.dbo.r_ss.v)")
        tdSql.execute(
            "CREATE STABLE vstb_ss_runtime (ts timestamp, v bigint) "
            "TAGS (backend nchar(16)) VIRTUAL 1")
        tdSql.execute(
            f"CREATE VTABLE vctb_ss ("
            f"v FROM {_SS_SRC}.{_SS_DB}.dbo.r_ss_child.v) "
            "USING vstb_ss_runtime TAGS ('sqlserver')")

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_PG_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_INF_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_SS_SRC}")

    def setup_method(self, method):
        tdSql.execute(f"USE {_LOCAL_DB}")

    def test_pg_normal_vtable_query_fails_when_backend_down_and_recovers(self):
        """PostgreSQL-backed normal vtable query fails while backend is down and recovers after restart.

        PostgreSQL-backed normal vtable query fails while backend is down and recovers after restart.

        Since: v3.4.3.0

        """
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
        """InfluxDB-backed normal vtable query fails while backend is down and recovers after restart.

        InfluxDB-backed normal vtable query fails while backend is down and recovers after restart.

        Since: v3.4.3.0

        """
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
        """Virtual stable query fails if one child backend is down; it must not return partial rows.

        Virtual stable query fails if one child backend is down; it must not return partial rows.

        Since: v3.4.3.0

        """
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

    def test_sqlserver_normal_vtable_query_fails_when_backend_down_and_recovers(self):
        """SQL Server-backed normal vtable query fails while backend is down and recovers after restart.

        SQL Server-backed normal vtable query fails while backend is down and recovers after restart.

        Since: v3.4.4.0

        """
        _check_count("SELECT count(*) FROM v_ss_normal", 2)
        ExtSrcEnv.stop_sqlserver_instance(_sqlserver_ver())
        try:
            tdSql.error("SELECT count(*) FROM v_ss_normal")
            tdSql.query("DESCRIBE v_ss_normal")
            # The persisted column source keeps source/database/schema/table/column.
            col_source = str(tdSql.getData(1, 4))
            assert col_source.startswith(f"{_SS_SRC}.{_SS_DB}."), col_source
            assert col_source.endswith(".r_ss.v"), col_source
        finally:
            ExtSrcEnv.start_sqlserver_instance(_sqlserver_ver())
        _check_count("SELECT count(*) FROM v_ss_normal", 2)

    def test_sqlserver_stable_query_fails_when_child_backend_down_and_recovers(self):
        """Virtual stable query fails if the SQL Server child backend is down and recovers after restart.

        Virtual stable query fails if the SQL Server child backend is down and recovers after restart.

        Since: v3.4.4.0

        """
        _check_count("SELECT count(*) FROM vstb_ss_runtime", 2)
        _check_count("SELECT count(*) FROM vctb_ss", 2)

        ExtSrcEnv.stop_sqlserver_instance(_sqlserver_ver())
        try:
            tdSql.error("SELECT count(*) FROM vstb_ss_runtime")
            # An unrelated child backend stays queryable while SQL Server is down.
            _check_count("SELECT count(*) FROM vctb_pg", 2)
            tdSql.query("DESCRIBE vstb_ss_runtime")
            tdSql.checkData(1, 0, "v")
        finally:
            ExtSrcEnv.start_sqlserver_instance(_sqlserver_ver())

        _check_count("SELECT count(*) FROM vstb_ss_runtime", 2)
