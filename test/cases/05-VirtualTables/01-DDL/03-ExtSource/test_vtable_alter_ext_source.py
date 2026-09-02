###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

"""DDL tests for ALTER VTABLE / ALTER STABLE … VIRTUAL referencing ext sources.

Section 4 of the DDL test plan.
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
    create_mysql_table, create_influx_measurement)


_LOCAL_DB = "valter_local"
_PG_DB    = "valter_pg"
_PG_SRC   = "valter_pg_src"
_MY_DB    = "valter_my"
_MY_SRC   = "valter_my_src"
_INF_DB   = "valter_inf"
_INF_SRC  = "valter_inf_src"


def _ensure_pg_source_and_tables():
    create_remote_db("postgresql", _PG_DB)
    create_pg_table(_PG_DB, "r1",
                    "ts TIMESTAMP PRIMARY KEY, v INTEGER, w INTEGER",
                    ["('2024-01-01 00:00:00', 1, 10)",
                     "('2024-01-01 00:01:00', 2, 20)"])
    create_pg_table(_PG_DB, "r2",
                    "ts TIMESTAMP PRIMARY KEY, v INTEGER, x DOUBLE PRECISION",
                    ["('2024-01-01 00:00:00', 100, 1.5)",
                     "('2024-01-01 00:01:00', 200, 2.5)"])
    create_ext_source(_PG_SRC, "postgresql", _PG_DB)


def _ensure_mysql_source_and_tables():
    create_remote_db("mysql", _MY_DB)
    create_mysql_table(_MY_DB, "r1",
                       "ts DATETIME(3) NOT NULL PRIMARY KEY, v INT, w INT",
                       ["('2024-01-01 00:00:00.000', 1, 10)",
                        "('2024-01-01 00:01:00.000', 2, 20)"])
    create_ext_source(_MY_SRC, "mysql", _MY_DB)


def _check_describe_layout(table, expected):
    """Verify DESCRIBE output matches expected layout.

    Each element in `expected` is (name, type) or (name, type, ref).
    When ref is provided, column 4 (the ref field) is also checked.
    """
    tdSql.query(f"DESCRIBE {table}")
    tdSql.checkRows(len(expected))
    for row, item in enumerate(expected):
        tdSql.checkData(row, 0, item[0])
        tdSql.checkData(row, 1, item[1])
        if len(item) >= 3:
            tdSql.checkData(row, 4, item[2])


# ===========================================================================

class TestVtableAlterExtSource:

    @classmethod
    def setup_class(cls):
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()
        _ensure_pg_source_and_tables()
        _ensure_mysql_source_and_tables()
        create_remote_db("influxdb", _INF_DB)
        create_influx_measurement(_INF_DB, [
            "r_inftag,device=sensor1 value=42.0 1700000000000000000"
        ])
        create_ext_source(_INF_SRC, "influxdb", _INF_DB)
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"CREATE DATABASE {_LOCAL_DB} PRECISION 'ms'")
        tdSql.execute(f"USE {_LOCAL_DB}")

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_PG_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_MY_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_INF_SRC}")

    # -------------------------------------------------------------------
    # 4.1 — Normal vtable
    # -------------------------------------------------------------------

    def _fresh_normal_vt(self, name="v_alter"):
        tdSql.execute(f"DROP VTABLE IF EXISTS {name}")
        tdSql.execute(
            f"CREATE VTABLE {name} (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.r1.v)")
        return name

    def test_alter_normal_vtable(self):
        """§4.1 — ALTER on normal vtable: add/drop/modify/rename columns."""
        # (a) ADD COLUMN with FROM — succeeds and ref visible in DESCRIBE.
        tdLog.info("§4.1a: add column with FROM succeeds")
        n = self._fresh_normal_vt()
        tdSql.execute(
            f"ALTER VTABLE {n} ADD COLUMN w int "
            f"FROM {_PG_SRC}.{_PG_DB}.r1.w")
        _check_describe_layout(n, [
            ("ts", "TIMESTAMP", ""),
            ("v", "INT", f"{_PG_SRC}.{_PG_DB}.r1.v"),
            ("w", "INT", f"{_PG_SRC}.{_PG_DB}.r1.w"),
        ])
        tdSql.execute(f"DROP VTABLE IF EXISTS {n}")

        # (b) ADD COLUMN without FROM — column always NULL.
        tdLog.info("§4.1b: add column without FROM is NULL")
        n = self._fresh_normal_vt("v_alter_null")
        tdSql.execute(f"ALTER VTABLE {n} ADD COLUMN nu int")
        _check_describe_layout(n, [
            ("ts", "TIMESTAMP", ""),
            ("v", "INT", f"{_PG_SRC}.{_PG_DB}.r1.v"),
            ("nu", "INT", ""),
        ])
        tdSql.execute(f"DROP VTABLE IF EXISTS {n}")

        # (c) DROP COLUMN that has ext ref — succeeds.
        tdLog.info("§4.1c: drop column with ext ref succeeds")
        n = self._fresh_normal_vt("v_alter_drop")
        tdSql.execute(f"ALTER VTABLE {n} ADD COLUMN w int FROM {_PG_SRC}.{_PG_DB}.r1.w")
        tdSql.execute(f"ALTER VTABLE {n} DROP COLUMN w")
        _check_describe_layout(n, [
            ("ts", "TIMESTAMP", ""),
            ("v", "INT", f"{_PG_SRC}.{_PG_DB}.r1.v"),
        ])
        tdSql.execute(f"DROP VTABLE IF EXISTS {n}")

        # (d) MODIFY COLUMN widening/narrowing.
        tdLog.info("§4.1d: modify column widening/narrowing")
        tdSql.execute("DROP VTABLE IF EXISTS v_alter_mod")
        create_pg_table(_PG_DB, "r_mod",
                        "ts TIMESTAMP PRIMARY KEY, s VARCHAR(64)")
        tdSql.execute(
            f"CREATE VTABLE v_alter_mod (ts timestamp, "
            f"s nchar(16) FROM {_PG_SRC}.{_PG_DB}.r_mod.s)")
        tdSql.error("ALTER VTABLE v_alter_mod MODIFY COLUMN s nchar(64)")
        tdSql.error("ALTER VTABLE v_alter_mod MODIFY COLUMN s nchar(4)")
        tdSql.execute("DROP VTABLE IF EXISTS v_alter_mod")

        # (e) RENAME COLUMN preserves FROM binding.
        tdLog.info("§4.1e: rename column preserves FROM")
        n = self._fresh_normal_vt("v_alter_rename")
        tdSql.execute(f"ALTER VTABLE {n} RENAME COLUMN v vrenamed")
        _check_describe_layout(n, [
            ("ts", "TIMESTAMP", ""),
            ("vrenamed", "INT", f"{_PG_SRC}.{_PG_DB}.r1.v"),
        ])
        tdSql.execute(f"DROP VTABLE IF EXISTS {n}")

        # (f) ALTER referencing a dropped source fails.
        tdLog.info("§4.1f: alter after source dropped fails")
        n = self._fresh_normal_vt("v_alter_dropsrc")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_PG_SRC}")
        tdSql.error(
            f"ALTER VTABLE {n} ADD COLUMN x int "
            f"FROM {_PG_SRC}.{_PG_DB}.r1.w")
        _ensure_pg_source_and_tables()
        tdSql.execute(f"USE {_LOCAL_DB}")
        tdSql.execute(f"DROP VTABLE IF EXISTS {n}")

        # (g) ADD COLUMN referencing an InfluxDB tag (not field) is rejected.
        tdLog.info("§4.1g: influxdb tag ref rejected")
        n = self._fresh_normal_vt("v_alter_inftag")
        tdSql.error(
            f"ALTER VTABLE {n} ADD COLUMN d nchar(64) "
            f"FROM {_INF_SRC}.{_INF_DB}.r_inftag.device")
        tdSql.execute(f"DROP VTABLE IF EXISTS {n}")

    # -------------------------------------------------------------------
    # 4.2 — vstb
    # -------------------------------------------------------------------

    def _fresh_vstb(self, stb="vstb_alter"):
        tdSql.execute(f"DROP STABLE IF EXISTS {stb}")
        tdSql.execute(
            f"CREATE STABLE {stb} (ts timestamp, v int) "
            f"TAGS (site nchar(16)) VIRTUAL 1")
        tdSql.execute(
            f"CREATE VTABLE vctb_alter ("
            f"v FROM {_PG_SRC}.{_PG_DB}.r1.v) "
            f"USING {stb} TAGS ('siteA')")
        return stb

    def test_vstb_add_column_propagates(self):
        stb = self._fresh_vstb("vstb_alter_add")
        tdSql.execute(f"ALTER STABLE {stb} ADD COLUMN w int")
        _check_describe_layout("vctb_alter", [
            ("ts", "TIMESTAMP", ""),
            ("v", "INT", f"{_PG_SRC}.{_PG_DB}.r1.v"),
            ("w", "INT", ""),
            ("site", "NCHAR"),
        ])
        tdSql.query("SELECT count(*) FROM vctb_alter WHERE w IS NULL")
        n_null = tdSql.getData(0, 0)
        tdSql.query("SELECT count(*) FROM vctb_alter")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)
        tdLog.info(f"[contract] vstb ADD COLUMN w propagation: null={n_null}")
        tdSql.execute("DROP VTABLE IF EXISTS vctb_alter")
        tdSql.execute(f"DROP STABLE IF EXISTS {stb}")

    # -------------------------------------------------------------------
    # 4.4 — External source lifecycle and refreshed remote schema
    # -------------------------------------------------------------------

    def test_existing_vtable_refs_after_source_refresh_drop_and_recreate(self):
        """Existing refs survive refresh, fail after source drop, and recover after recreate."""
        tdSql.execute("DROP VTABLE IF EXISTS v_refresh_pg")
        tdSql.execute(
            f"CREATE VTABLE v_refresh_pg (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.r1.v)")
        tdSql.query("SELECT count(*) FROM v_refresh_pg")
        tdSql.checkData(0, 0, 2)
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
        tdSql.query("SELECT count(*) FROM v_refresh_pg")
        tdSql.checkData(0, 0, 2)
        tdSql.execute(f"DROP EXTERNAL SOURCE {_PG_SRC}")
        tdSql.error("SELECT count(*) FROM v_refresh_pg")
        create_ext_source(_PG_SRC, "postgresql", _PG_DB)
        tdSql.query("SELECT count(*) FROM v_refresh_pg")
        tdSql.checkData(0, 0, 2)
        tdSql.execute("DROP VTABLE IF EXISTS v_refresh_pg")

        tdSql.execute("DROP VTABLE IF EXISTS v_refresh_my")
        tdSql.execute(
            f"CREATE VTABLE v_refresh_my (ts timestamp, "
            f"v int FROM {_MY_SRC}.{_MY_DB}.r1.v)")
        tdSql.query("SELECT count(*) FROM v_refresh_my")
        tdSql.checkData(0, 0, 2)
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_MY_SRC}")
        tdSql.query("SELECT count(*) FROM v_refresh_my")
        tdSql.checkData(0, 0, 2)
        tdSql.execute(f"DROP EXTERNAL SOURCE {_MY_SRC}")
        tdSql.error("SELECT count(*) FROM v_refresh_my")
        create_ext_source(_MY_SRC, "mysql", _MY_DB)
        tdSql.query("SELECT count(*) FROM v_refresh_my")
        tdSql.checkData(0, 0, 2)
        tdSql.execute("DROP VTABLE IF EXISTS v_refresh_my")

        tdSql.execute("DROP VTABLE IF EXISTS v_refresh_inf")
        tdSql.execute(
            f"CREATE VTABLE v_refresh_inf (ts timestamp, "
            f"value double FROM {_INF_SRC}.{_INF_DB}.r_inftag.value)")
        tdSql.query("SELECT count(*) FROM v_refresh_inf")
        tdSql.checkData(0, 0, 1)
        tdSql.execute(f"DROP EXTERNAL SOURCE {_INF_SRC}")
        tdSql.error("SELECT count(*) FROM v_refresh_inf")
        create_ext_source(_INF_SRC, "influxdb", _INF_DB)
        tdSql.query("SELECT count(*) FROM v_refresh_inf")
        tdSql.checkData(0, 0, 1)
        tdSql.execute("DROP VTABLE IF EXISTS v_refresh_inf")

    def test_alter_column_set_ref_after_remote_schema_change(self):
        """Ref binding sees refreshed remote schema and rejects incompatible type changes."""
        create_pg_table(_PG_DB, "r_mut",
                        "ts TIMESTAMP PRIMARY KEY, v INTEGER",
                        ["('2024-01-01 00:00:00', 1)"])
        tdSql.execute("DROP VTABLE IF EXISTS v_mut_pg")
        tdSql.execute(
            f"CREATE VTABLE v_mut_pg (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.r_mut.v, w int)")
        tdSql.error(f"ALTER VTABLE v_mut_pg ALTER COLUMN w SET {_PG_SRC}.{_PG_DB}.r_mut.w")
        ExtSrcEnv.pg_exec(_PG_DB, ["ALTER TABLE r_mut ADD COLUMN w INTEGER DEFAULT 7"])
        try:
            tdSql.execute(f"ALTER VTABLE v_mut_pg ALTER COLUMN w SET {_PG_SRC}.{_PG_DB}.r_mut.w")
        except Exception as err:
            tdLog.info(f"pre-refresh PG SET ref failed as allowed: {err}")
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
        tdSql.execute(f"ALTER VTABLE v_mut_pg ALTER COLUMN w SET {_PG_SRC}.{_PG_DB}.r_mut.w")
        tdSql.query("SELECT w FROM v_mut_pg")
        tdSql.checkData(0, 0, 7)
        ExtSrcEnv.pg_exec(_PG_DB, [
            "ALTER TABLE r_mut ALTER COLUMN w TYPE TEXT USING w::TEXT",
            "UPDATE r_mut SET w = 'bad'",
        ])
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_PG_SRC}")
        tdSql.error("SELECT w FROM v_mut_pg")
        tdSql.execute("ALTER VTABLE v_mut_pg ALTER COLUMN w SET NULL")
        tdSql.error(f"ALTER VTABLE v_mut_pg ALTER COLUMN w SET {_PG_SRC}.{_PG_DB}.r_mut.w")
        tdSql.execute("DROP VTABLE IF EXISTS v_mut_pg")

        create_mysql_table(_MY_DB, "r_mut",
                           "ts DATETIME(3) NOT NULL PRIMARY KEY, v INT",
                           ["('2024-01-01 00:00:00.000', 1)"])
        tdSql.execute("DROP VTABLE IF EXISTS v_mut_my")
        tdSql.execute(
            f"CREATE VTABLE v_mut_my (ts timestamp, "
            f"v int FROM {_MY_SRC}.{_MY_DB}.r_mut.v, w int)")
        tdSql.error(f"ALTER VTABLE v_mut_my ALTER COLUMN w SET {_MY_SRC}.{_MY_DB}.r_mut.w")
        ExtSrcEnv.mysql_exec(_MY_DB, ["ALTER TABLE r_mut ADD COLUMN w INT DEFAULT 7"])
        try:
            tdSql.execute(f"ALTER VTABLE v_mut_my ALTER COLUMN w SET {_MY_SRC}.{_MY_DB}.r_mut.w")
        except Exception as err:
            tdLog.info(f"pre-refresh MySQL SET ref failed as allowed: {err}")
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_MY_SRC}")
        tdSql.execute(f"ALTER VTABLE v_mut_my ALTER COLUMN w SET {_MY_SRC}.{_MY_DB}.r_mut.w")
        tdSql.query("SELECT w FROM v_mut_my")
        tdSql.checkData(0, 0, 7)
        ExtSrcEnv.mysql_exec(_MY_DB, [
            "ALTER TABLE r_mut MODIFY COLUMN w VARCHAR(32)",
            "UPDATE r_mut SET w = 'bad'",
        ])
        tdSql.execute(f"REFRESH EXTERNAL SOURCE {_MY_SRC}")
        tdSql.error("SELECT w FROM v_mut_my")
        tdSql.execute("ALTER VTABLE v_mut_my ALTER COLUMN w SET NULL")
        tdSql.error(f"ALTER VTABLE v_mut_my ALTER COLUMN w SET {_MY_SRC}.{_MY_DB}.r_mut.w")
        tdSql.execute("DROP VTABLE IF EXISTS v_mut_my")

    def test_vstb_drop_column_propagates(self):
        stb = self._fresh_vstb("vstb_alter_drop")
        tdSql.execute(f"ALTER STABLE {stb} ADD COLUMN w int")
        tdSql.execute(f"ALTER STABLE {stb} DROP COLUMN w")
        _check_describe_layout("vctb_alter", [
            ("ts", "TIMESTAMP", ""),
            ("v", "INT", f"{_PG_SRC}.{_PG_DB}.r1.v"),
            ("site", "NCHAR"),
        ])
        tdSql.execute("DROP VTABLE IF EXISTS vctb_alter")
        tdSql.execute(f"DROP STABLE IF EXISTS {stb}")

    def test_vstb_modify_column(self):
        stb = self._fresh_vstb("vstb_alter_mod")
        tdSql.execute(f"ALTER STABLE {stb} ADD COLUMN s varchar(16)")
        tdSql.execute(f"ALTER STABLE {stb} MODIFY COLUMN s varchar(64)")
        _check_describe_layout("vctb_alter", [
            ("ts", "TIMESTAMP", ""),
            ("v", "INT", f"{_PG_SRC}.{_PG_DB}.r1.v"),
            ("s", "VARCHAR", ""),
            ("site", "NCHAR"),
        ])
        tdSql.error(f"ALTER STABLE {stb} MODIFY COLUMN s varchar(4)")
        tdSql.execute("DROP VTABLE IF EXISTS vctb_alter")
        tdSql.execute(f"DROP STABLE IF EXISTS {stb}")

    def test_vstb_add_drop_tag(self):
        stb = self._fresh_vstb("vstb_alter_tag")
        tdSql.execute(f"ALTER STABLE {stb} ADD TAG newt int")
        tdSql.execute(f"ALTER STABLE {stb} DROP TAG newt")
        tdSql.execute("DROP VTABLE vctb_alter")
        tdSql.execute(f"DROP STABLE {stb}")

    # -------------------------------------------------------------------
    # 4.3 — Child tag SET
    # -------------------------------------------------------------------

    def test_child_set_tag_single(self):
        stb = self._fresh_vstb("vstb_alter_tags")
        tdSql.execute("ALTER VTABLE vctb_alter SET TAG site = 'siteB'")
        tdSql.query("SELECT site FROM vctb_alter LIMIT 1")
        tdSql.checkData(0, 0, "siteB")
        tdSql.execute("DROP VTABLE vctb_alter")
        tdSql.execute(f"DROP STABLE {stb}")

    def test_child_set_tag_type_mismatch(self):
        stb = self._fresh_vstb("vstb_alter_tag_mis")
        # site is nchar(16); set a too-long string → must fail.
        tdSql.error(
            "ALTER VTABLE vctb_alter SET TAG site = "
            "'this_is_a_string_clearly_longer_than_16_chars'")
        tdSql.execute("ALTER VTABLE vctb_alter SET TAG site = 12345")
        tdSql.query("SELECT site FROM vctb_alter LIMIT 1")
        tdSql.checkData(0, 0, "12345")
        tdSql.execute("DROP VTABLE IF EXISTS vctb_alter")
        tdSql.execute(f"DROP STABLE IF EXISTS {stb}")
