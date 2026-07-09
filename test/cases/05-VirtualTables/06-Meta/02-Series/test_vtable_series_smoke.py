###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""
End-to-end smoke test for the VTABLE SERIES pipeline.

Exercises in a single test:
  - Parser:        CREATE VTABLE ... SERIES ...
  - Meta encode:   SVCreateTbReq.series → SMetaEntry.series
  - SHOW CREATE:   series clauses rendered back out
  - Client round-trip: SELECT triggers STableMetaRsp path
                       (numOfSeries/pSeries through to STableMeta)
  - ALTER VTABLE ADD/REMOVE SERIES + meta refresh
"""

import os
import sys
import time

from new_test_framework.utils import tdLog, tdSql

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from vtable_util import ExtSrcEnv


DB_NAME     = "test_series_smoke"
EXT_SOURCE  = "series_smoke_src"
INFLUX_DB   = "series_smoke_influx"
MEASUREMENT = "cpu_metrics"


class TestVtableSeriesSmoke:
    """Single-shot smoke check that the SERIES pipeline is wired end-to-end."""

    @classmethod
    def setup_class(cls):
        ExtSrcEnv.start_influx_instance(ExtSrcEnv.INFLUX_VERSIONS[0])
        ExtSrcEnv.ensure_qnode()

        ExtSrcEnv.influx_drop_db(INFLUX_DB)
        ExtSrcEnv.influx_create_db(INFLUX_DB)

        ts_ns = 1716000000 * 1_000_000_000
        ExtSrcEnv.influx_write(INFLUX_DB, [
            f"{MEASUREMENT},host=srv01,region=us cpu=55.0,mem=72.0 {ts_ns}",
            f"{MEASUREMENT},host=srv02,region=eu cpu=33.0,mem=60.0 {ts_ns + 10**9}",
        ])
        time.sleep(2)

        tdSql.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
        tdSql.execute(f"CREATE DATABASE {DB_NAME}")
        tdSql.execute(f"USE {DB_NAME}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {EXT_SOURCE}")
        tdSql.execute(
            f"CREATE EXTERNAL SOURCE IF NOT EXISTS {EXT_SOURCE} "
            f"TYPE='influxdb' "
            f"HOST='{ExtSrcEnv.INFLUX_HOST}' PORT={ExtSrcEnv.INFLUX_PORT} "
            f"USER='u' PASSWORD='' DATABASE={INFLUX_DB} "
            f"OPTIONS('api_token'='{ExtSrcEnv._get_influx_token(ExtSrcEnv.INFLUX_VERSIONS[0])}','protocol'='flight_sql')"
        )

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {EXT_SOURCE}")

    # ------------------------------------------------------------------
    @staticmethod
    def _get_create_sql():
        for c in range(tdSql.queryCols):
            v = tdSql.getData(0, c)
            if isinstance(v, str) and "CREATE" in v.upper():
                return v
        tdLog.exit("no CREATE statement in SHOW CREATE output")
        return ""

    def _show_create(self, name):
        tdSql.query(f"SHOW CREATE VTABLE {name}")
        tdSql.checkRows(1)
        return self._get_create_sql()

    # ------------------------------------------------------------------
    def test_series_pipeline_smoke(self):
        tdLog.info("=== SERIES pipeline smoke test ===")
        tdSql.execute(f"USE {DB_NAME}")

        # --- 1. CREATE VTABLE with SERIES --------------------------------
        tdSql.execute(
            f"CREATE VTABLE vt_smoke ("
            f"  ts  TIMESTAMP,"
            f"  cpu DOUBLE FROM s1.cpu,"
            f"  mem DOUBLE FROM s1.mem"
            f") SERIES s1 AS {EXT_SOURCE}.{INFLUX_DB}.{MEASUREMENT} "
            f"(host='srv01', region='us')"
        )
        tdLog.info("  [1] CREATE VTABLE with SERIES OK")

        # --- 2. SHOW CREATE renders SERIES clause ------------------------
        sql = self._show_create("vt_smoke")
        assert "SERIES" in sql.upper(),     f"missing SERIES clause: {sql}"
        assert "s1" in sql,                  f"missing series alias s1: {sql}"
        assert MEASUREMENT in sql,           f"missing measurement: {sql}"
        tdLog.info("  [2] SHOW CREATE contains SERIES clause OK")

        # --- 3. SELECT round-trips STableMetaRsp with series -------------
        tdSql.query("SELECT count(*) FROM vt_smoke")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdLog.info("  [3] SELECT meta round-trip OK")

        # --- 4. ALTER ADD SERIES ----------------------------------------
        tdSql.execute(
            f"ALTER VTABLE vt_smoke ADD SERIES s2 AS "
            f"{EXT_SOURCE}.{INFLUX_DB}.{MEASUREMENT} "
            f"(host='srv02', region='eu')"
        )
        sql = self._show_create("vt_smoke")
        assert "s1" in sql and "s2" in sql, f"ADD SERIES not reflected: {sql}"
        tdLog.info("  [4] ALTER ADD SERIES reflected in SHOW CREATE OK")

        # SELECT again to make sure refreshed meta survives the round-trip
        tdSql.query("SELECT count(*) FROM vt_smoke")

        # --- 5. ALTER REMOVE SERIES -------------------------------------
        tdSql.execute("ALTER VTABLE vt_smoke REMOVE SERIES s2")
        sql = self._show_create("vt_smoke")
        assert "s1" in sql,        f"s1 should remain: {sql}"
        assert "s2" not in sql,    f"s2 should be gone: {sql}"
        tdLog.info("  [5] ALTER REMOVE SERIES reflected in SHOW CREATE OK")

        tdSql.query("SELECT count(*) FROM vt_smoke")
        tdLog.info("=== SERIES pipeline smoke test PASSED ===")
