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

# -*- coding: utf-8 -*-
"""
Test that SERIES metadata is properly propagated through STableMetaRsp
to the client's STableMeta.

Verifies the full round-trip:
  1. CREATE VTABLE with SERIES clause
  2. SHOW CREATE VTABLE returns SERIES info
  3. Client can get table meta (via query) without crash
  4. ALTER TABLE ADD/REMOVE SERIES updates meta correctly
"""

import os
import sys
import time

from new_test_framework.utils import tdLog, tdSql, etool, tdCom

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from vtable_util import ExtSrcEnv


DB_NAME = "test_series_meta"
EXT_SOURCE = "series_meta_src"
INFLUX_DB = "series_meta_influx"
MEASUREMENT = "cpu_metrics"


class TestVtableSeriesMeta:
    """Test series metadata in table meta response."""

    @classmethod
    def setup_class(cls):
        ExtSrcEnv.start_influx_instance(ExtSrcEnv.INFLUX_VERSIONS[0])
        ExtSrcEnv.ensure_qnode()

        # Setup InfluxDB database
        ExtSrcEnv.influx_drop_db(INFLUX_DB)
        ExtSrcEnv.influx_create_db(INFLUX_DB)

        # Write minimal data so measurement exists
        ts_ns = 1716000000 * 1_000_000_000
        lines = [
            f"{MEASUREMENT},host=srv01,region=us cpu=55.0,mem=72.0 {ts_ns}",
            f"{MEASUREMENT},host=srv02,region=eu cpu=33.0,mem=60.0 {ts_ns + 1000000000}",
        ]
        ExtSrcEnv.influx_write(INFLUX_DB, lines)
        time.sleep(3)

        # Setup TDengine
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
        tdSql.execute(f"CREATE DATABASE {DB_NAME} PRECISION 'ns'")
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
        # DROP EXTERNAL SOURCE only removes the local definition.  The InfluxDB
        # bucket outlives it and keeps occupying one of the five database slots
        # InfluxDB 3 Core allows per instance, so a full run that visits several
        # ext-source suites eventually makes every later influx_create_db()
        # fail with HTTP 422.
        ExtSrcEnv.influx_drop_db(INFLUX_DB)

    # ------------------------------------------------------------------
    # Test 1: Create vtable with single series, verify meta round-trip
    # ------------------------------------------------------------------
    def test_single_series_meta(self):
        """CREATE VTABLE with one SERIES projects its columns and round-trips the metadata

        A virtual normal table backed by one SERIES: SHOW CREATE must round-trip the SERIES clause
        with its alias and measurement, DESCRIBE must expose the projected columns, and an
        aggregate query must drive the table-meta path the client catalog caches.

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        Labels: virtual, series, meta, external_source

        Jira: None

        History:
            - 2026-09-14 Added the mandatory docstring fields required by the docstring CI check
        """
        tdLog.info("=== test_single_series_meta ===")
        tdSql.execute(f"USE {DB_NAME}")

        tdSql.execute(
            f"CREATE VTABLE vt_single ("
            f"  ts TIMESTAMP,"
            f"  cpu DOUBLE FROM s1.cpu,"
            f"  mem DOUBLE FROM s1.mem"
            f") SERIES s1 AS {EXT_SOURCE}.{INFLUX_DB}.{MEASUREMENT} "
            f"(host='srv01', region='us')"
        )

        # Verify SHOW CREATE contains SERIES info
        tdSql.query("SHOW CREATE VTABLE vt_single")
        tdSql.checkRows(1)
        create_sql = None
        for c in range(tdSql.queryCols):
            v = tdSql.getData(0, c)
            if isinstance(v, str) and "CREATE" in v.upper():
                create_sql = v
                break
        assert create_sql is not None, "SHOW CREATE should return a CREATE statement"
        assert "SERIES" in create_sql.upper(), \
            f"SHOW CREATE should contain SERIES clause, got: {create_sql}"
        assert "s1" in create_sql, \
            f"SHOW CREATE should contain series alias 's1', got: {create_sql}"
        assert MEASUREMENT in create_sql, \
            f"SHOW CREATE should contain measurement name, got: {create_sql}"
        tdLog.info(f"  SHOW CREATE OK: contains SERIES clause")

        # Verify client can get table meta through the aggregate query path.
        tdSql.query("SELECT count(*) FROM vt_single")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdLog.info("  Query OK: count(*) returned 1 row")

        # Verify DESCRIBE shows ref info
        tdSql.query("DESCRIBE vt_single")
        found_cpu = False
        for i in range(tdSql.queryRows):
            if tdSql.getData(i, 0) and tdSql.getData(i, 0).strip() == "cpu":
                found_cpu = True
                break
        assert found_cpu, "DESCRIBE should show 'cpu' column"
        tdLog.info("  DESCRIBE OK")

    # ------------------------------------------------------------------
    # Test 2: Create vtable with multiple series
    # ------------------------------------------------------------------
    def test_multi_series_meta(self):
        """CREATE VTABLE with two SERIES from different tag conditions

        Two SERIES aliases over the same measurement with disjoint tag conditions: SHOW CREATE must
        list both aliases and DESCRIBE must expose the column projected from each SERIES, so a
        per-series alias is never collapsed onto another one.

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        Labels: virtual, series, meta, external_source

        Jira: None

        History:
            - 2026-09-14 Added the mandatory docstring fields required by the docstring CI check
        """
        tdLog.info("=== test_multi_series_meta ===")
        tdSql.execute(f"USE {DB_NAME}")

        tdSql.execute(
            f"CREATE VTABLE vt_multi ("
            f"  ts TIMESTAMP,"
            f"  us_cpu DOUBLE FROM s_us.cpu,"
            f"  eu_cpu DOUBLE FROM s_eu.cpu"
            f") SERIES s_us AS {EXT_SOURCE}.{INFLUX_DB}.{MEASUREMENT} "
            f"(host='srv01', region='us') "
            f"SERIES s_eu AS {EXT_SOURCE}.{INFLUX_DB}.{MEASUREMENT} "
            f"(host='srv02', region='eu')"
        )

        # Verify SHOW CREATE contains both series
        tdSql.query("SHOW CREATE VTABLE vt_multi")
        create_sql = None
        for c in range(tdSql.queryCols):
            v = tdSql.getData(0, c)
            if isinstance(v, str) and "CREATE" in v.upper():
                create_sql = v
                break
        assert create_sql is not None
        assert "s_us" in create_sql, f"should contain s_us: {create_sql}"
        assert "s_eu" in create_sql, f"should contain s_eu: {create_sql}"
        tdLog.info("  Multi-series SHOW CREATE OK")

        tdSql.query("DESCRIBE vt_multi")
        found_us = False
        found_eu = False
        for i in range(tdSql.queryRows):
            field = tdSql.getData(i, 0)
            if field and field.strip() == "us_cpu":
                found_us = True
            if field and field.strip() == "eu_cpu":
                found_eu = True
        assert found_us, "DESCRIBE should show 'us_cpu' column"
        assert found_eu, "DESCRIBE should show 'eu_cpu' column"
        tdLog.info("  Multi-series DESCRIBE OK")

    # ------------------------------------------------------------------
    # Test 3: Virtual child table with series
    # ------------------------------------------------------------------
    def test_child_table_series_meta(self):
        """A virtual child table may declare SERIES and inherit the metadata through its parent

        A virtual child table under a virtual super table declares its own SERIES: SHOW CREATE must
        keep the SERIES clause, querying the child exercises the catalog cache merge path, and
        querying through the super table exercises the batch path.

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        Labels: virtual, series, meta, external_source

        Jira: None

        History:
            - 2026-09-14 Added the mandatory docstring fields required by the docstring CI check
        """
        tdLog.info("=== test_child_table_series_meta ===")
        tdSql.execute(f"USE {DB_NAME}")

        # Create virtual super table
        tdSql.execute(
            "CREATE STABLE vstb_series (ts TIMESTAMP, cpu DOUBLE, mem DOUBLE) "
            "TAGS (site NCHAR(32)) VIRTUAL 1"
        )

        # Create virtual child table with SERIES
        tdSql.execute(
            f"CREATE VTABLE vctb_s1 ("
            f"  cpu FROM sr.cpu,"
            f"  mem FROM sr.mem"
            f") USING vstb_series TAGS ('site_us') "
            f"SERIES sr AS {EXT_SOURCE}.{INFLUX_DB}.{MEASUREMENT} "
            f"(host='srv01', region='us')"
        )

        # SHOW CREATE should show SERIES
        tdSql.query("SHOW CREATE VTABLE vctb_s1")
        create_sql = None
        for c in range(tdSql.queryCols):
            v = tdSql.getData(0, c)
            if isinstance(v, str) and "CREATE" in v.upper():
                create_sql = v
                break
        assert create_sql is not None
        assert "SERIES" in create_sql.upper(), \
            f"Child table SHOW CREATE should have SERIES: {create_sql}"
        tdLog.info("  Child table SHOW CREATE OK")

        # Query child table (triggers catalog cache merge path)
        tdSql.query("SELECT count(*) FROM vctb_s1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdLog.info("  Child table query OK: count(*) returned 1 row")

        # Query via super table (triggers ctgCache batch path)
        tdSql.query("SELECT count(*) FROM vstb_series")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdLog.info("  Super table query OK: count(*) returned 1 row")

    # ------------------------------------------------------------------
    # Test 4: ALTER VTABLE ADD/REMOVE SERIES, verify meta update
    # ------------------------------------------------------------------
    def test_alter_series_meta(self):
        """ALTER VTABLE ADD / REMOVE SERIES keeps SHOW CREATE and queries consistent

        Adding a SERIES must be reflected in SHOW CREATE while the existing alias survives, and
        REMOVE SERIES must drop only the removed alias; both must leave the table queryable, which
        is what exercises the meta refresh path after an ADD SERIES meta response.

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        Labels: virtual, series, meta, external_source

        Jira: None

        History:
            - 2026-09-14 Added the mandatory docstring fields required by the docstring CI check
        """
        tdLog.info("=== test_alter_series_meta ===")
        tdSql.execute(f"USE {DB_NAME}")

        # Start with a vtable that has one series
        tdSql.execute(
            f"CREATE VTABLE vt_alter ("
            f"  ts TIMESTAMP,"
            f"  cpu DOUBLE FROM s1.cpu"
            f") SERIES s1 AS {EXT_SOURCE}.{INFLUX_DB}.{MEASUREMENT} "
            f"(host='srv01', region='us')"
        )

        # Verify initial state
        tdSql.query("SHOW CREATE VTABLE vt_alter")
        create_sql = self._get_create_sql()
        assert "s1" in create_sql
        tdLog.info("  Initial SHOW CREATE OK")

        # ADD another series
        tdSql.execute(
            f"ALTER VTABLE vt_alter ADD SERIES s2 AS "
            f"{EXT_SOURCE}.{INFLUX_DB}.{MEASUREMENT} "
            f"(host='srv02', region='eu')"
        )

        # Verify both series are in SHOW CREATE
        tdSql.query("SHOW CREATE VTABLE vt_alter")
        create_sql = self._get_create_sql()
        assert "s1" in create_sql, f"should still have s1: {create_sql}"
        assert "s2" in create_sql, f"should now have s2: {create_sql}"
        tdLog.info("  After ADD SERIES: SHOW CREATE shows both series")

        # Query still works (meta refresh path)
        tdSql.query("SELECT count(cpu) FROM vt_alter")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdLog.info("  Query after ADD SERIES OK: count(cpu) returned 1 row")

        # REMOVE series s2 (no columns reference it)
        tdSql.execute("ALTER VTABLE vt_alter REMOVE SERIES s2")

        # Verify only s1 remains
        tdSql.query("SHOW CREATE VTABLE vt_alter")
        create_sql = self._get_create_sql()
        assert "s1" in create_sql, f"should still have s1: {create_sql}"
        assert "s2" not in create_sql, f"should no longer have s2: {create_sql}"
        tdLog.info("  After REMOVE SERIES: SHOW CREATE shows only s1")

        # Query still works
        tdSql.query("SELECT count(cpu) FROM vt_alter")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdLog.info("  Query after REMOVE SERIES OK: count(cpu) returned 1 row")

    # ------------------------------------------------------------------
    # Test 5: ADD SERIES must not drop the owned tag schema from the response
    # ------------------------------------------------------------------
    def test_add_series_keeps_owned_tag(self):
        """ADD SERIES keeps the owned-tag schema in the meta response

        metaAddTableSeries advances colRef.version (rversion), which is enough for
        ctgWriteTbMetaToCache to accept its response; the response used to carry the row schema and
        colRef only, so the client cache was replaced by a tag-less meta and any later tag access
        on the same connection failed with "Invalid tag name". A virtual normal table can own tags
        and hold series at the same time, so the response must carry the owned tag schema just like
        every other normal / virtual-normal meta response.

        Catalog:
            - VirtualTable

        Since: v3.4.2.0

        Labels: virtual, series, meta, tag, regression

        Jira: None

        History:
            - 2026-09-14 Created: regression for the tag-less ADD SERIES meta response
        """
        tdLog.info("=== test_add_series_keeps_owned_tag ===")
        tdSql.execute(f"USE {DB_NAME}")

        # Build the table in two statements that each have a precedent in this suite, rather than
        # one combined DDL: for a virtual NORMAL table the grammar is
        #   NK_LP column_def_list NK_RP series_clause_opt vtags_def_opt       (sql.y:1109)
        # i.e. SERIES comes BEFORE TAGS -- the opposite of a virtual CHILD table, whose form is
        # "... USING vstb TAGS (...) series_clause_opt". The combined spelling is therefore easy to
        # get wrong, and mixing SERIES with an owned tag is not what this case is about.
        tdSql.execute(
            f"CREATE VTABLE vt_series_tag ("
            f"  ts TIMESTAMP,"
            f"  cpu DOUBLE FROM s1.cpu"
            f") SERIES s1 AS {EXT_SOURCE}.{INFLUX_DB}.{MEASUREMENT} "
            f"(host='srv01', region='us')"
        )
        # ADD TAG on a normal / virtual-normal table is the vnode path (buildAddTagReq gates on
        # exactly those two table types); the tag starts NULL and SET TAG gives it a literal value.
        tdSql.execute("ALTER TABLE vt_series_tag ADD TAG own_t INT")
        tdSql.execute("ALTER TABLE vt_series_tag SET TAG own_t = 3")

        # a tag read works before the ALTER, so the cache starts out complete
        tdSql.query("SELECT DISTINCT own_t FROM vt_series_tag")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)
        tdLog.info("  Pre-ALTER owned tag read OK")

        tdSql.execute(
            f"ALTER VTABLE vt_series_tag ADD SERIES s2 AS "
            f"{EXT_SOURCE}.{INFLUX_DB}.{MEASUREMENT} "
            f"(host='srv02', region='eu')"
        )

        # the owned tag must still resolve on this very connection: the ADD SERIES response is
        # what refreshed the cache, so a tag-less response would break the read right here
        tdSql.query("SELECT DISTINCT own_t FROM vt_series_tag")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)
        tdLog.info("  Owned tag survives ADD SERIES")

        # and the tag is still writable, not just readable
        tdSql.execute("ALTER TABLE vt_series_tag SET TAG own_t = 8")
        tdSql.query("SELECT DISTINCT own_t FROM vt_series_tag")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 8)
        tdLog.info("  SET TAG after ADD SERIES OK")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _get_create_sql(self):
        """Extract CREATE statement from last SHOW CREATE query result."""
        for c in range(tdSql.queryCols):
            v = tdSql.getData(0, c)
            if isinstance(v, str) and "CREATE" in v.upper():
                return v
        tdLog.exit("no CREATE statement in SHOW CREATE output")
        return ""
