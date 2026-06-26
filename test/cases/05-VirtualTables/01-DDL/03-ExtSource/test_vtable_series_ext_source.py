###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

"""DDL tests for the InfluxDB-only SERIES feature on virtual tables.

Covers what the sibling ext-source DDL files do NOT:
  A. CREATE VTABLE ... SERIES (normal vtable, single / multi series alias)
  B. CREATE VTABLE ... USING vstb ... SERIES (child vtable)
  C. ALTER VTABLE ADD SERIES / REMOVE SERIES
  D. ALTER COLUMN col SET alias.col  (colref <-> series)
  E. Negative paths (non-influx source, tag-match violations, unknown alias)
  F. InfluxDB-side tag-set growth after a series is created

Each success scenario is followed by a SELECT smoke check that the series
pin returns the expected rows. All InfluxDB data is written once in
setup_class with a single WAL->parquet snapshot wait; Group F writes one
extra evolving-tag batch with its own wait.
"""

# -*- coding: utf-8 -*-
import os
import sys
import time

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

# --- names ---
_LOCAL_DB = "vseries_local"
_INF_DB   = "vseries_inf"
_INF_SRC  = "vseries_inf_src"
_INF_SRC_GROWTH = "vseries_inf_src_growth"
_PG_DB    = "vseries_pg"
_PG_SRC   = "vseries_pg_src"

# Nanosecond timestamps (InfluxDB line protocol precision = ns).
_T0 = 1700000000000000000


def _check_count(sql, expected):
    tdSql.query(sql)
    tdSql.checkData(0, 0, expected)


def _write_setup_data():
    """Write all base InfluxDB measurements, then one snapshot wait."""
    create_remote_db("influxdb", _INF_DB)

    lines = []
    # shared_m: tag device, fields value(f64), extra(i64). d1=3 rows, d2=2 rows.
    for i in range(3):
        lines.append(f"shared_m,device=d1 value={1.0 + i},extra={10 + i}i "
                     f"{_T0 + i * 1000000000}")
    for i in range(2):
        lines.append(f"shared_m,device=d2 value={5.0 + i},extra={50 + i}i "
                     f"{_T0 + i * 1000000000}")
    # m2: tags host,region; field v. host=h1,region=cn = 2 rows.
    for i in range(2):
        lines.append(f"m2,host=h1,region=cn v={100 + i}i "
                     f"{_T0 + i * 1000000000}")
    # m_notag: field only, no tags. 1 row.
    lines.append(f"m_notag value=7.0 {_T0}")
    # m_evolve: tag device only at setup. d1 = 2 rows.
    for i in range(2):
        lines.append(f"m_evolve,device=d1 value={20.0 + i} "
                     f"{_T0 + i * 1000000000}")

    create_influx_measurement(_INF_DB, lines)
    # Force a snapshot so FlightSQL can read the rows.
    create_influx_measurement(
        _INF_DB, ["_snapshot_trigger_ value=1i 1000000000000000000"])


def _write_pg_setup_data():
    """Create a real non-Influx source so negative SERIES type checks are not false positives."""
    create_remote_db("postgresql", _PG_DB)
    create_pg_table(
        _PG_DB, "r",
        "ts TIMESTAMP PRIMARY KEY, v INTEGER",
        ["('2024-01-01 00:00:00', 1)"])


# ===========================================================================

class TestVtableSeriesExtSource:

    _evolve_grown = False

    @classmethod
    def setup_class(cls):
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()
        _write_setup_data()
        _write_pg_setup_data()
        create_ext_source(_INF_SRC, "influxdb", _INF_DB)
        create_ext_source(_PG_SRC, "postgresql", _PG_DB)
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"CREATE DATABASE {_LOCAL_DB} PRECISION 'ms'")
        tdSql.execute(f"USE {_LOCAL_DB}")

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_INF_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_INF_SRC_GROWTH}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_PG_SRC}")

    def setup_method(self, method):
        tdSql.execute(f"USE {_LOCAL_DB}")

    # ===================================================================
    # A. CREATE VTABLE + SERIES (normal vtable)
    # ===================================================================

    def test_create_vtable_series_on_normal_vtable(self):
        """Create normal vtables with InfluxDB SERIES bindings and validate row sets."""
        tdSql.execute("DROP VTABLE IF EXISTS v_a1")
        tdSql.execute(
            "CREATE VTABLE v_a1 (ts timestamp, "
            "value double FROM s1.value, extra bigint FROM s1.extra) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1')")
        tdSql.query("DESCRIBE v_a1")
        tdSql.checkData(1, 0, "value")
        tdSql.checkData(
            1, 4, "vseries_inf_src.vseries_inf.shared_m.value (device='d1')")
        _check_count("SELECT count(*) FROM v_a1", 3)
        tdSql.execute("DROP VTABLE v_a1")

        tdSql.execute("DROP VTABLE IF EXISTS v_a2")
        tdSql.execute(
            "CREATE VTABLE v_a2 (ts timestamp, "
            "v1 double FROM s1.value, v2 double FROM s2.value) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1') "
            "SERIES s2 AS vseries_inf_src.vseries_inf.shared_m (device='d2')")
        tdSql.query("DESCRIBE v_a2")
        tdSql.checkData(1, 0, "v1")
        tdSql.checkData(
            1, 4, "vseries_inf_src.vseries_inf.shared_m.value (device='d1')")
        tdSql.checkData(2, 0, "v2")
        tdSql.checkData(
            2, 4, "vseries_inf_src.vseries_inf.shared_m.value (device='d2')")
        # Distinct series → counts of their own pinned rows.
        _check_count("SELECT count(v1) FROM v_a2", 3)
        _check_count("SELECT count(v2) FROM v_a2", 2)
        tdSql.execute("DROP VTABLE v_a2")

        tdSql.execute("DROP VTABLE IF EXISTS v_a3")
        tdSql.execute(
            "CREATE VTABLE v_a3 (ts timestamp, "
            "value double FROM s1.value, local_only int) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1')")
        tdSql.query("DESCRIBE v_a3")
        tdSql.checkData(2, 0, "local_only")
        tdSql.checkData(2, 4, "")
        _check_count("SELECT count(*) FROM v_a3", 3)
        _check_count("SELECT count(local_only) FROM v_a3", 0)
        tdSql.execute("DROP VTABLE v_a3")

        tdSql.execute("DROP VTABLE IF EXISTS v_a4")
        tdSql.execute(
            "CREATE VTABLE v_a4 (ts timestamp, v bigint FROM s1.v) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.m2 (host='h1', region='cn')")
        tdSql.query("DESCRIBE v_a4")
        tdSql.checkData(1, 0, "v")
        tdSql.checkData(
            1, 4, "vseries_inf_src.vseries_inf.m2.v (host='h1',region='cn')")
        _check_count("SELECT count(*) FROM v_a4", 2)
        tdSql.execute("DROP VTABLE v_a4")

        tdSql.execute("DROP VTABLE IF EXISTS v_a5")
        tdSql.execute(
            "CREATE VTABLE IF NOT EXISTS v_a5 (ts timestamp, "
            "value double FROM s1.value) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1')")
        tdSql.execute(
            "CREATE VTABLE IF NOT EXISTS v_a5 (ts timestamp, "
            "value double FROM s1.value) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1')")
        _check_count("SELECT count(*) FROM v_a5", 3)
        tdSql.execute("DROP VTABLE v_a5")

    # ===================================================================
    # B. CREATE VTABLE ... USING vstb + SERIES (child vtable)
    # ===================================================================

    def _fresh_vstb(self, stb):
        tdSql.execute(f"DROP STABLE IF EXISTS {stb}")
        tdSql.execute(
            f"CREATE STABLE {stb} (ts timestamp, value double) "
            f"TAGS (site nchar(16)) VIRTUAL 1")

    def test_create_child_vtable_series_using_vstb(self):
        """Create child vtables with SERIES under a virtual stable and query them."""
        self._fresh_vstb("stb_b1")
        tdSql.execute("DROP VTABLE IF EXISTS vc_b1")
        tdSql.execute(
            "CREATE VTABLE vc_b1 (value FROM s1.value) "
            "USING stb_b1 TAGS ('siteA') "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1')")
        tdSql.query("DESCRIBE vc_b1")
        tdSql.checkData(1, 0, "value")
        tdSql.checkData(
            1, 4, "vseries_inf_src.vseries_inf.shared_m.value (device='d1')")
        _check_count("SELECT count(*) FROM vc_b1", 3)
        tdSql.execute("DROP VTABLE vc_b1")
        tdSql.execute("DROP STABLE stb_b1")

        self._fresh_vstb("stb_b2")
        tdSql.execute("DROP VTABLE IF EXISTS vc_b2_d1")
        tdSql.execute("DROP VTABLE IF EXISTS vc_b2_d2")
        tdSql.execute(
            "CREATE VTABLE vc_b2_d1 (value FROM s1.value) "
            "USING stb_b2 TAGS ('siteD1') "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1')")
        tdSql.execute(
            "CREATE VTABLE vc_b2_d2 (value FROM s1.value) "
            "USING stb_b2 TAGS ('siteD2') "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d2')")
        _check_count("SELECT count(*) FROM vc_b2_d1", 3)
        _check_count("SELECT count(*) FROM vc_b2_d2", 2)
        _check_count("SELECT count(*) FROM stb_b2", 5)
        tdSql.execute("DROP VTABLE vc_b2_d1")
        tdSql.execute("DROP VTABLE vc_b2_d2")
        tdSql.execute("DROP STABLE stb_b2")

        self._fresh_vstb("stb_b3")
        tdSql.execute("DROP VTABLE IF EXISTS vc_b3")
        tdSql.execute(
            "CREATE VTABLE vc_b3 (s1.value) "
            "USING stb_b3 TAGS ('sitePos') "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1')")
        tdSql.query("DESCRIBE vc_b3")
        tdSql.checkData(1, 0, "value")
        tdSql.checkData(
            1, 4, "vseries_inf_src.vseries_inf.shared_m.value (device='d1')")
        _check_count("SELECT count(*) FROM vc_b3", 3)
        tdSql.execute("DROP VTABLE vc_b3")
        tdSql.execute("DROP STABLE stb_b3")

        self._fresh_vstb("stb_b4")
        tdSql.execute("DROP VTABLE IF EXISTS vc_b4")
        tdSql.execute(
            "CREATE VTABLE vc_b4 USING stb_b4 TAGS ('siteDeferred') "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1')")
        _check_count("SELECT count(value) FROM vc_b4", 0)
        tdSql.execute("ALTER VTABLE vc_b4 ALTER COLUMN value SET s1.value")
        tdSql.query("DESCRIBE vc_b4")
        tdSql.checkData(1, 0, "value")
        tdSql.checkData(
            1, 4, "vseries_inf_src.vseries_inf.shared_m.value (device='d1')")
        _check_count("SELECT count(*) FROM vc_b4", 3)
        tdSql.execute("DROP VTABLE vc_b4")
        tdSql.execute("DROP STABLE stb_b4")

    # ===================================================================
    # C. ALTER VTABLE ADD SERIES / REMOVE SERIES
    # ===================================================================

    def test_alter_vtable_add_and_remove_series_aliases(self):
        """Alter vtables to add or remove SERIES aliases and verify the bound rows."""
        tdSql.execute("DROP VTABLE IF EXISTS v_c1")
        # Start with a local-only NULL column (no series yet).
        tdSql.execute("CREATE VTABLE v_c1 (ts timestamp, value double)")
        _check_count("SELECT count(value) FROM v_c1", 0)
        tdSql.execute(
            "ALTER VTABLE v_c1 ADD SERIES s1 AS vseries_inf_src.vseries_inf.shared_m "
            "(device='d1')")
        tdSql.execute("ALTER VTABLE v_c1 ALTER COLUMN value SET s1.value")
        tdSql.query("DESCRIBE v_c1")
        tdSql.checkData(1, 0, "value")
        tdSql.checkData(
            1, 4, "vseries_inf_src.vseries_inf.shared_m.value (device='d1')")
        _check_count("SELECT count(value) FROM v_c1", 3)
        tdSql.execute("DROP VTABLE v_c1")

        tdSql.execute("DROP VTABLE IF EXISTS v_c2")
        tdSql.execute(
            "CREATE VTABLE v_c2 (ts timestamp, value double FROM s1.value) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1')")
        _check_count("SELECT count(value) FROM v_c2", 3)
        tdSql.error("ALTER VTABLE v_c2 REMOVE SERIES s1")
        tdSql.execute("ALTER VTABLE v_c2 ALTER COLUMN value SET NULL")
        tdSql.execute("ALTER VTABLE v_c2 REMOVE SERIES s1")
        tdSql.query("DESCRIBE v_c2")
        tdSql.checkData(1, 0, "value")
        tdSql.checkData(1, 4, "")
        _check_count("SELECT count(value) FROM v_c2", 0)
        tdSql.execute("DROP VTABLE v_c2")

        tdSql.execute("DROP VTABLE IF EXISTS v_c3")
        tdSql.execute(
            "CREATE VTABLE v_c3 (ts timestamp, v1 double FROM s1.value) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1')")
        tdSql.execute("ALTER VTABLE v_c3 ADD COLUMN v2 double")
        tdSql.execute(
            "ALTER VTABLE v_c3 ADD SERIES s2 AS vseries_inf_src.vseries_inf.shared_m "
            "(device='d2')")
        tdSql.execute("ALTER VTABLE v_c3 ALTER COLUMN v2 SET s2.value")
        _check_count("SELECT count(v1) FROM v_c3", 3)
        _check_count("SELECT count(v2) FROM v_c3", 2)
        tdSql.execute("DROP VTABLE v_c3")

    # ===================================================================
    # D. ALTER COLUMN: colref <-> series
    # ===================================================================

    def test_alter_vtable_column_bindings_between_null_and_series(self):
        """Alter column bindings between NULL and different SERIES aliases."""
        tdSql.execute("DROP VTABLE IF EXISTS v_d1")
        tdSql.execute("CREATE VTABLE v_d1 (ts timestamp, value double)")
        _check_count("SELECT count(value) FROM v_d1", 0)
        tdSql.execute(
            "ALTER VTABLE v_d1 ADD SERIES s1 AS vseries_inf_src.vseries_inf.shared_m "
            "(device='d1')")
        tdSql.execute("ALTER VTABLE v_d1 ALTER COLUMN value SET s1.value")
        _check_count("SELECT count(value) FROM v_d1", 3)
        tdSql.execute("DROP VTABLE v_d1")

        tdSql.execute("DROP VTABLE IF EXISTS v_d2")
        tdSql.execute(
            "CREATE VTABLE v_d2 (ts timestamp, value double FROM s1.value) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1')")
        _check_count("SELECT count(value) FROM v_d2", 3)
        tdSql.execute("ALTER VTABLE v_d2 ALTER COLUMN value SET NULL")
        _check_count("SELECT count(value) FROM v_d2", 0)
        tdSql.execute("DROP VTABLE v_d2")

        tdSql.execute("DROP VTABLE IF EXISTS v_d3")
        tdSql.execute(
            "CREATE VTABLE v_d3 (ts timestamp, value double FROM s1.value) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1') "
            "SERIES s2 AS vseries_inf_src.vseries_inf.shared_m (device='d2')")
        _check_count("SELECT count(value) FROM v_d3", 3)
        tdSql.execute("ALTER VTABLE v_d3 ALTER COLUMN value SET s2.value")
        tdSql.query("DESCRIBE v_d3")
        tdSql.checkData(1, 0, "value")
        tdSql.checkData(
            1, 4, "vseries_inf_src.vseries_inf.shared_m.value (device='d2')")
        _check_count("SELECT count(value) FROM v_d3", 2)
        tdSql.execute("DROP VTABLE v_d3")

    # ===================================================================
    # E. Negative paths
    # ===================================================================

    def test_reject_invalid_series_ddl(self):
        """Reject invalid SERIES DDL against non-Influx sources and bad tag conditions."""
        tdSql.error(
            "CREATE VTABLE v_e1 (ts timestamp, v int FROM s1.v) "
            f"SERIES s1 AS {_PG_SRC}.{_PG_DB}.r (v='1')")

        """E2: m2 has tags host+region; pinning only host is rejected."""
        tdSql.error(
            "CREATE VTABLE v_e2 (ts timestamp, v bigint FROM s1.v) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.m2 (host='h1')")

        """E3: pinning a tag the measurement does not have is rejected."""
        tdSql.error(
            "CREATE VTABLE v_e3 (ts timestamp, value double FROM s1.value) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1', nope='x')")

        """E4: the same tag specified twice is rejected."""
        tdSql.error(
            "CREATE VTABLE v_e4 (ts timestamp, value double FROM s1.value) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1', device='d1')")

        """E5: a non tag='value' condition is rejected."""
        tdSql.error(
            "CREATE VTABLE v_e5 (ts timestamp, value double FROM s1.value) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device>'d1')")

        """E6: an empty tag condition list is rejected (syntax/semantic)."""
        tdSql.error(
            "CREATE VTABLE v_e6 (ts timestamp, value double FROM s1.value) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m ()")

        """E7: a column referencing an alias with no SERIES decl is rejected."""
        tdSql.error(
            "CREATE VTABLE v_e7 (ts timestamp, value double FROM sX.value) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1')")

        """E8: REMOVE SERIES for a non-existent alias is rejected."""
        tdSql.execute(
            "CREATE VTABLE v_e8 (ts timestamp, value double FROM s1.value) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1')")
        tdSql.error("ALTER VTABLE v_e8 REMOVE SERIES nope")
        tdSql.execute("DROP VTABLE v_e8")

        """E9: ADD SERIES with an already-used alias is rejected."""
        tdSql.execute(
            "CREATE VTABLE v_e9 (ts timestamp, value double FROM s1.value) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1')")
        tdSql.error("ALTER VTABLE v_e9 ADD SERIES s1 AS "
                    "vseries_inf_src.vseries_inf.shared_m (device='d2')")
        tdSql.execute("DROP VTABLE v_e9")

        """E10: referencing an InfluxDB tag column as a field is rejected."""
        tdSql.error(
            "CREATE VTABLE v_e10 (ts timestamp, d nchar(16) FROM s1.device) "
            "SERIES s1 AS vseries_inf_src.vseries_inf.shared_m (device='d1')")

        """E11: ALTER ADD SERIES rejects a non-Influx source."""
        tdSql.execute("CREATE VTABLE v_e11 (ts timestamp, value double)")
        tdSql.error(
            "ALTER VTABLE v_e11 ADD SERIES s1 AS "
            "vseries_pg_src.vseries_pg.r (v='1')")
        tdSql.execute("DROP VTABLE v_e11")

        """E12: ALTER ADD SERIES still validates the complete tag set."""
        tdSql.execute("CREATE VTABLE v_e12 (ts timestamp, value double)")
        tdSql.error(
            "ALTER VTABLE v_e12 ADD SERIES s1 AS "
            "vseries_inf_src.vseries_inf.m2 (host='h1')")
        tdSql.execute("DROP VTABLE v_e12")

    # ===================================================================
    # F. InfluxDB-side tag-set growth after a series is created
    # ===================================================================

    # Pre-existing series vtable on m_evolve (tag set = {device}) kept alive
    # across the tag-growth so F3 can re-query it.
    def _ensure_evolve_baseline_vtable(self):
        tdSql.query(
            "SELECT count(*) FROM information_schema.ins_tables "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_f_base'")
        if tdSql.getData(0, 0) == 0:
            tdSql.execute(
                "CREATE VTABLE v_f_base (ts timestamp, "
                "value double FROM s1.value) "
                "SERIES s1 AS vseries_inf_src.vseries_inf.m_evolve (device='d1')")

    def _grow_evolve_tags_once(self):
        """Write a row to m_evolve with an extra tag `zone`, then snapshot."""
        if getattr(self, "_evolve_grown", False):
            return
        create_influx_measurement(_INF_DB, [
            f"m_evolve,device=d1,zone=z1 value=99.0 {_T0 + 5 * 1000000000}"])
        create_influx_measurement(
            _INF_DB, ["_snapshot_trigger2_ value=1i 1000000000000000000"])
        self.__class__._evolve_grown = True

    def _ensure_growth_source(self):
        """Create a fresh source alias so the latest measurement schema is re-read."""
        create_ext_source(_INF_SRC_GROWTH, "influxdb", _INF_DB)

    def _ensure_evolve_baseline_vstb(self):
        tdSql.query(
            "SELECT count(*) FROM information_schema.ins_stables "
            f"WHERE db_name='{_LOCAL_DB}' AND stable_name='stb_f_base'")
        if tdSql.getData(0, 0) == 0:
            self._fresh_vstb("stb_f_base")
        tdSql.query(
            "SELECT count(*) FROM information_schema.ins_tables "
            f"WHERE db_name='{_LOCAL_DB}' AND table_name='vc_f_base'")
        if tdSql.getData(0, 0) == 0:
            tdSql.execute(
                "CREATE VTABLE vc_f_base (value FROM s1.value) "
                "USING stb_f_base TAGS ('siteF') "
                "SERIES s1 AS vseries_inf_src.vseries_inf.m_evolve (device='d1')")

    def test_keep_series_semantics_after_influx_tag_growth(self):
        """Keep existing SERIES semantics after InfluxDB grows its tag set."""
        self._ensure_evolve_baseline_vtable()    # original source caches {device}
        self._grow_evolve_tags_once()            # InfluxDB tags -> {device, zone}
        self._ensure_growth_source()             # fresh source sees current tags
        tdSql.execute("DROP VTABLE IF EXISTS v_f1")
        tdSql.error(
            "CREATE VTABLE v_f1 (ts timestamp, value double FROM s1.value) "
            f"SERIES s1 AS {_INF_SRC_GROWTH}.{_INF_DB}.m_evolve (device='d1')")

        self._ensure_evolve_baseline_vtable()   # original source caches {device}
        self._grow_evolve_tags_once()
        self._ensure_growth_source()
        tdSql.execute("DROP VTABLE IF EXISTS v_f2")
        tdSql.execute(
            "CREATE VTABLE v_f2 (ts timestamp, value double FROM s1.value) "
            f"SERIES s1 AS {_INF_SRC_GROWTH}.{_INF_DB}.m_evolve "
            "(device='d1', zone='z1')")
        tdSql.query("DESCRIBE v_f2")
        tdSql.checkData(1, 0, "value")
        tdSql.checkData(
            1, 4, "vseries_inf_src_growth.vseries_inf.m_evolve.value "
            "(device='d1',zone='z1')")
        _check_count("SELECT count(*) FROM v_f2", 1)
        tdSql.execute("DROP VTABLE v_f2")

        self._ensure_evolve_baseline_vtable()
        self._grow_evolve_tags_once()
        tdSql.query("SELECT count(*) FROM v_f_base")
        tdSql.checkData(0, 0, 2)
        tdSql.execute("DROP VTABLE IF EXISTS v_f_base")

        self._ensure_evolve_baseline_vstb()
        self._grow_evolve_tags_once()
        tdSql.query("SELECT count(*) FROM vc_f_base")
        tdSql.checkData(0, 0, 2)
        tdSql.query("SELECT count(*) FROM stb_f_base")
        tdSql.checkData(0, 0, 2)
        tdSql.execute("DROP VTABLE IF EXISTS vc_f_base")
        tdSql.execute("DROP STABLE IF EXISTS stb_f_base")
