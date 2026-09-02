###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

"""DDL meta-info tests for vtables / vstbs referencing ext sources.

Section 6 of the DDL test plan: DESCRIBE, SHOW CREATE, INFORMATION_SCHEMA.
"""

# -*- coding: utf-8 -*-
import os
import sys

from new_test_framework.utils import tdLog, tdSql

_FQ_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__),
                 "..", "..", "..", "09-DataQuerying", "19-FederatedQuery"))
if _FQ_DIR not in sys.path:
    sys.path.insert(0, _FQ_DIR)
from federated_query_common import ExtSrcEnv  # noqa: E402
from ext_source_helpers import (  # noqa: E402
    create_ext_source, create_remote_db, create_pg_table,
    create_influx_measurement)


_LOCAL_DB = "vmeta_local"
_PG_DB    = "vmeta_pg"
_PG_SRC   = "vmeta_pg_src"
_INF_DB   = "vmeta_inf"
_INF_SRC  = "vmeta_inf_src"


def _ensure_env():
    create_remote_db("postgresql", _PG_DB)
    create_pg_table(_PG_DB, "r",
                    "ts TIMESTAMP PRIMARY KEY, v INTEGER, w INTEGER",
                    ["('2024-01-01 00:00:00', 1, 10)"])
    create_ext_source(_PG_SRC, "postgresql", _PG_DB)
    create_remote_db("influxdb", _INF_DB)
    create_influx_measurement(_INF_DB, [
        "m_meta,device=d1 value=1.0 1700000000000000000",
        "m_meta,device=d1 value=2.0 1700000001000000000",
    ])
    create_influx_measurement(
        _INF_DB, ["_snapshot_trigger_meta value=1i 1000000000000000000"])
    create_ext_source(_INF_SRC, "influxdb", _INF_DB)


def _extract_show_create_sql(sql):
    tdSql.query(sql)
    tdSql.checkRows(1)
    for col in range(tdSql.queryCols):
        value = tdSql.getData(0, col)
        if isinstance(value, str) and "CREATE" in value.upper():
            return value
    tdLog.exit(f"no CREATE statement in SHOW output: {sql}")


# ===========================================================================

class TestVtableMetaExtSource:

    @classmethod
    def setup_class(cls):
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()
        _ensure_env()
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"CREATE DATABASE {_LOCAL_DB} PRECISION 'ms'")
        tdSql.execute(f"USE {_LOCAL_DB}")

        # One normal vtable + one vstb with one child for the whole class.
        tdSql.execute(
            f"CREATE VTABLE v_meta (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.r.v, "
            f"w int FROM {_PG_SRC}.{_PG_DB}.r.w, "
            f"local_only int)")
        tdSql.execute(
            "CREATE STABLE vstb_meta (ts timestamp, v int) "
            "TAGS (site nchar(16)) VIRTUAL 1")
        tdSql.execute(
            f"CREATE VTABLE vctb_meta ("
            f"v FROM {_PG_SRC}.{_PG_DB}.r.v) "
            f"USING vstb_meta TAGS ('siteA')")
        tdSql.execute(
            "CREATE VTABLE v_series_meta (ts timestamp, "
            "value double FROM s1.value) "
            f"SERIES s1 AS {_INF_SRC}.{_INF_DB}.m_meta (device='d1')")
        tdSql.execute(
            "CREATE STABLE vstb_series_meta (ts timestamp, value double) "
            "TAGS (site nchar(16)) VIRTUAL 1")
        tdSql.execute(
            "CREATE VTABLE vctb_series_meta (value FROM s1.value) "
            "USING vstb_series_meta TAGS ('siteSeries') "
            f"SERIES s1 AS {_INF_SRC}.{_INF_DB}.m_meta (device='d1')")

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_PG_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_INF_SRC}")

    # -------------------------------------------------------------------
    # 6.1 — Normal vtable meta
    # -------------------------------------------------------------------

    def test_describe_vtable_shows_columns(self):
        tdSql.query("DESCRIBE v_meta")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "v")
        tdSql.checkData(2, 0, "w")
        tdSql.checkData(3, 0, "local_only")

    def test_show_create_vtable_roundtrip(self):
        tdSql.query("SELECT table_name FROM information_schema.ins_tables "
                    f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_meta'")
        tdSql.checkRows(1)
        sql = _extract_show_create_sql("SHOW CREATE VTABLE v_meta")
        if _PG_SRC not in sql or _PG_DB not in sql or "local_only" not in sql:
            tdLog.exit(f"expected {_PG_SRC}, {_PG_DB} and local_only in {sql}")
        tdSql.execute("DROP VTABLE IF EXISTS v_meta_rt")
        tdSql.execute(sql.replace("v_meta", "v_meta_rt"))
        tdSql.query("DESCRIBE v_meta_rt")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "v")
        tdSql.checkData(2, 0, "w")
        tdSql.checkData(3, 0, "local_only")
        tdSql.execute("DROP VTABLE v_meta_rt")

    def test_information_schema_ins_tables(self):
        tdSql.query("SELECT table_name, `type` FROM information_schema.ins_tables "
                    f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_meta'")
        tdSql.checkRows(1)
        table_type = str(tdSql.getData(0, 1)).upper()
        if "VIRTUAL" not in table_type:
            tdLog.exit(f"expected VIRTUAL in {table_type}")

    def test_information_schema_ins_columns_from_ref(self):
        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_meta' "
            "AND col_name IN ('local_only', 'ts', 'v', 'w') "
            "ORDER BY col_name")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "local_only")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "ts")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "v")
        tdSql.checkData(2, 1, f"{_PG_SRC}.{_PG_DB}.r.v")
        tdSql.checkData(3, 0, "w")
        tdSql.checkData(3, 1, f"{_PG_SRC}.{_PG_DB}.r.w")

    def test_refresh_external_source_preserves_existing_metadata(self):
        sql_before = _extract_show_create_sql("SHOW CREATE VTABLE v_meta")
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
        sql_after = _extract_show_create_sql("SHOW CREATE VTABLE v_meta")
        if sql_before != sql_after:
            tdLog.exit(f"SHOW CREATE changed after REFRESH: before={sql_before}, after={sql_after}")

        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_meta' "
            "AND col_name IN ('v', 'w') ORDER BY col_name")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "v")
        tdSql.checkData(0, 1, f"{_PG_SRC}.{_PG_DB}.r.v")
        tdSql.checkData(1, 0, "w")
        tdSql.checkData(1, 1, f"{_PG_SRC}.{_PG_DB}.r.w")

    def test_show_create_quoted_identifier_vtable_roundtrip(self):
        create_pg_table(_PG_DB, '"MetaCaseTable"',
                        'ts TIMESTAMP PRIMARY KEY, "MixedValue" INTEGER',
                        ["('2024-01-01 00:00:00', 33)"])
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
        tdSql.execute("DROP VTABLE IF EXISTS v_meta_case")
        tdSql.execute(
            f"CREATE VTABLE v_meta_case (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.`MetaCaseTable`.`MixedValue`)")
        tdSql.query("SELECT v FROM v_meta_case")
        tdSql.checkData(0, 0, 33)

        sql = _extract_show_create_sql("SHOW CREATE VTABLE v_meta_case")
        tdSql.execute("DROP VTABLE IF EXISTS v_meta_case_rt")
        tdSql.execute(sql.replace("v_meta_case", "v_meta_case_rt"))
        tdSql.query("SELECT v FROM v_meta_case_rt")
        tdSql.checkData(0, 0, 33)
        tdSql.execute("DROP VTABLE IF EXISTS v_meta_case_rt")
        tdSql.execute("DROP VTABLE IF EXISTS v_meta_case")

    # -------------------------------------------------------------------
    # 6.2 — vstb meta
    # -------------------------------------------------------------------

    def test_describe_vstable(self):
        tdSql.query("DESCRIBE vstb_meta")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "v")
        tdSql.checkData(2, 0, "site")

    def test_show_create_stable_includes_virtual(self):
        sql = _extract_show_create_sql("SHOW CREATE STABLE vstb_meta")
        if "VIRTUAL" not in sql.upper():
            tdLog.exit(f"expected VIRTUAL in {sql}")
        # round-trip
        tdSql.execute("DROP STABLE IF EXISTS vstb_meta_rt")
        tdSql.execute(sql.replace("vstb_meta", "vstb_meta_rt"))
        tdSql.query(
            "SELECT count(*) FROM information_schema.ins_stables "
            f"WHERE db_name='{_LOCAL_DB}' AND stable_name='vstb_meta_rt'")
        tdSql.checkData(0, 0, 1)
        tdSql.execute("DROP STABLE vstb_meta_rt")

    def test_ins_stables_virtual_flag(self):
        tdSql.query("SELECT isvirtual FROM information_schema.ins_stables "
                    f"WHERE db_name='{_LOCAL_DB}' AND stable_name='vstb_meta'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, True)

    def test_child_vtable_ins_columns_shows_from(self):
        tdSql.query(
            "SELECT col_name, col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='vctb_meta' "
            "AND col_name='v'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "v")
        tdSql.checkData(0, 1, f"{_PG_SRC}.{_PG_DB}.r.v")

    def test_show_create_series_vtable_roundtrip(self):
        sql = _extract_show_create_sql("SHOW CREATE VTABLE v_series_meta")
        if "SERIES" not in sql.upper() or _INF_SRC not in sql or "device='d1'" not in sql:
            tdLog.exit(f"expected SERIES clause in {sql}")
        tdSql.execute("DROP VTABLE IF EXISTS v_series_meta_rt")
        tdSql.execute(sql.replace("v_series_meta", "v_series_meta_rt"))
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_series_meta_rt' "
            "AND col_name='value'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{_INF_SRC}.{_INF_DB}.m_meta.value")
        tdSql.query("SELECT count(*) FROM v_series_meta_rt")
        tdSql.checkData(0, 0, 2)
        tdSql.execute("DROP VTABLE v_series_meta_rt")

    def test_show_create_series_child_vtable_roundtrip(self):
        sql = _extract_show_create_sql("SHOW CREATE VTABLE vctb_series_meta")
        if "SERIES" not in sql.upper() or "USING `vstb_series_meta`" not in sql:
            tdLog.exit(f"expected USING + SERIES clause in {sql}")
        tdSql.execute("DROP VTABLE IF EXISTS vctb_series_meta_rt")
        tdSql.execute(sql.replace("vctb_series_meta", "vctb_series_meta_rt"))
        tdSql.query(
            "SELECT col_source FROM information_schema.ins_columns "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='vctb_series_meta_rt' "
            "AND col_name='value'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{_INF_SRC}.{_INF_DB}.m_meta.value")
        tdSql.query("SELECT count(*) FROM vctb_series_meta_rt")
        tdSql.checkData(0, 0, 2)
        tdSql.execute("DROP VTABLE vctb_series_meta_rt")
