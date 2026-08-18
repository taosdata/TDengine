###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved
#
#  This file is proprietary and confidential to TAOS Technologies, Inc.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""Tags on virtual NORMAL tables (CREATE VTABLE ... TAGS) — full query coverage.

Comprehensive suite for the virtual-normal-table tag feature, mirroring the depth
of test_vtable_tag_ref.py but for VIRTUAL_NORMAL_TABLE (a standalone virtual table).
Coverage across four dimensions:
  1. Data queries  — column projection, primary ts, WHERE on data/time, ORDER BY,
                     LIMIT/OFFSET, aggregates (COUNT/SUM/AVG/MIN/MAX), GROUP BY
  2. DESC          — full schema (columns + tags), types/lengths, ref columns
  3. SHOW CREATE   — owned / tag-ref / mixed / round-trip
  4. tag/tag-ref   — owned values (INT/NCHAR), tag-ref follow, multi-source,
                     filter (eq/range/IN), GROUP BY tag, DISTINCT, agg+tag-filter

Two tag mechanisms:
  - owned tags: inline `= value` at CREATE or set via SET TAG
  - tag-refs:   `FROM src.tag` references, value follows the source; SET TAG converts
                between the two forms (ref -> literal clears the ref, owned -> ref via
                SET TAG x = db.tb.tag)

Section 5 covers owned tags on PLAIN NORMAL tables (TSDB_NORMAL_TABLE):
ADD/SET/DROP TAG via ALTER, projection/filter/GROUP BY, DESC, SHOW CREATE,
and the negative cases (tag-ref FROM stays virtual-normal-table only).
Section 6 covers drop/re-add scenarios: same-name re-add (old value must not
resurrect), type change, first/middle/last positions, repeated cycles, and the
TSDB_MAX_TAGS cap.
"""

import time

import pytest

from new_test_framework.utils import tdLog, tdSql, tdDnodes

DB = "td_vntb_tags"
TS0 = 1700000000000  # base timestamp for source rows

# Source stable children: TAGS(city NCHAR(20), code INT, region NCHAR(16)) + data
SRC = {
    'src0': {'city': 'beijing',   'code': 100, 'region': 'east',  'data': [1, 2, 3, 4, 5, 6]},
    'src1': {'city': 'shanghai',  'code': 200, 'region': 'west',  'data': [10, 20, 30, 40]},
    'src2': {'city': 'guangzhou', 'code': 300, 'region': 'south', 'data': [100, 101, 102, 103, 104, 105, 106, 107]},
}


def _ts(j):
    return str(TS0 + j * 1000)


class TestVtableNormalTags:
    """Tags on virtual NORMAL tables — full query coverage (data/desc/show/tag)."""

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------
    def setup_class(cls):
        tdLog.info("=== setup: creating source DB/tables + shared virtual tables ===")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB};")
        tdSql.execute(f"CREATE DATABASE {DB} BUFFER 16;")
        tdSql.execute(f"USE {DB};")

        tdSql.execute("CREATE STABLE src_stb (ts TIMESTAMP, val INT) "
                      "TAGS (city NCHAR(20), code INT, region NCHAR(16));")
        for tbl, info in SRC.items():
            tdSql.execute(f"CREATE TABLE {tbl} USING src_stb "
                          f"TAGS ('{info['city']}', {info['code']}, '{info['region']}');")
        for tbl, info in SRC.items():
            for j, v in enumerate(info['data']):
                tdSql.execute(f"INSERT INTO {tbl} VALUES ({TS0 + j * 1000}, {v});")

        # Shared read-only virtual normal tables. DDL/lifecycle tests create their own.
        tdSql.execute("CREATE VTABLE v_own (ts TIMESTAMP, val INT FROM src0.val) "
                      "TAGS (loc INT = 5);")
        tdSql.execute("CREATE VTABLE v_ref (ts TIMESTAMP, val INT FROM src0.val) "
                      "TAGS (rcity NCHAR(20) FROM src0.city, rcode INT FROM src0.code);")
        tdSql.execute("CREATE VTABLE v_mixed (ts TIMESTAMP, val INT FROM src0.val) "
                      "TAGS (lit INT = 100, rcity NCHAR(20) FROM src0.city);")
        tdSql.execute("CREATE VTABLE v_mref (ts TIMESTAMP, val INT FROM src0.val) "
                      "TAGS (c0 NCHAR(20) FROM src0.city, c1 NCHAR(20) FROM src1.city, "
                      "c2 NCHAR(20) FROM src2.city);")
        tdSql.execute("CREATE VTABLE v_ntag (ts TIMESTAMP, val INT FROM src0.val) "
                      "TAGS (name NCHAR(16) = 'alpha', cnt INT = 7);")
        tdLog.info("=== setup complete ===")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _rows(self, sql):
        """Run a query, return rows as a list of str-tuples (order preserved)."""
        tdSql.query(sql)
        return [tuple(str(tdSql.getData(i, j)) for j in range(tdSql.queryCols))
                for i in range(tdSql.queryRows)]

    def _check_values(self, sql, expected, desc=""):
        """Assert rows match expected, order-independent (compared as strings)."""
        actual = sorted(self._rows(sql))
        exp = sorted(tuple(str(v) for v in r) for r in expected)
        nrows = len(actual)
        assert nrows == len(exp), f"{desc}: expected {len(exp)} rows, got {nrows}"
        assert actual == exp, f"{desc}: mismatch\n  expected {exp}\n  actual   {actual}"
        tdLog.info(f"  PASS: {desc}")

    def _check_ordered(self, sql, expected, desc=""):
        """Assert rows match expected, order-sensitive (for ORDER BY)."""
        actual = self._rows(sql)
        exp = [tuple(str(v) for v in r) for r in expected]
        assert actual == exp, f"{desc}: mismatch\n  expected {exp}\n  actual   {actual}"
        tdLog.info(f"  PASS: {desc}")

    def _check_count(self, sql, expected_count, desc=""):
        tdSql.query(sql)
        actual = tdSql.getData(0, 0)
        assert int(actual) == expected_count, f"{desc}: expected {expected_count}, got {actual}"
        tdLog.info(f"  PASS: {desc} (count={expected_count})")

    # ==================================================================
    # 1. DATA QUERIES (column references: val, ts)
    # ==================================================================

    def test_data_select_columns(self):
        """Data: SELECT ts, val returns all rows with values from the source.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, data

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        # CAST(ts AS BIGINT) yields the integer epoch ms so we can assert exact timestamps.
        self._check_values("SELECT CAST(ts AS BIGINT), val FROM v_own;",
                           [(_ts(j), d[j]) for j in range(len(d))], "select ts, val")

    def test_data_select_primary_ts(self):
        """Data: SELECT ts (primary timestamp) returns each row's timestamp.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, data

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values("SELECT CAST(ts AS BIGINT) FROM v_own;",
                           [(_ts(j),) for j in range(len(d))], "select primary ts")

    def test_data_select_data_column(self):
        """Data: SELECT val returns the source column values.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, data

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values("SELECT val FROM v_own;", [(v,) for v in d], "select data column")

    def test_data_filter_eq(self):
        """Data: WHERE val = const filters rows.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, data, filter

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        self._check_values("SELECT val FROM v_own WHERE val = 2;", [(2,)], "data eq filter")
        self._check_count("SELECT COUNT(*) FROM v_own WHERE val = 2;", 1, "data eq count")

    def test_data_filter_range(self):
        """Data: WHERE val > N / val BETWEEN filters rows.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, data, filter

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values("SELECT val FROM v_own WHERE val > 1;", [(v,) for v in d if v > 1],
                           "data > filter")
        self._check_values("SELECT val FROM v_own WHERE val >= 2;", [(v,) for v in d if v >= 2],
                           "data >= filter")
        self._check_values("SELECT val FROM v_own WHERE val BETWEEN 2 AND 3;",
                           [(v,) for v in d if 2 <= v <= 3], "data BETWEEN")

    def test_data_time_range_filter(self):
        """Data: WHERE ts filters by time range.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, data, filter, time

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values(f"SELECT val FROM v_own WHERE ts > {TS0};",
                           [(v,) for v in d[1:]], "ts > base")
        self._check_values(f"SELECT val FROM v_own WHERE ts >= {TS0};",
                           [(v,) for v in d], "ts >= base")
        self._check_count(f"SELECT COUNT(*) FROM v_own WHERE ts < {TS0 + 1000};", 1, "ts < count")

    def test_data_order_by_ts_asc(self):
        """Data: ORDER BY ts ASC returns rows in ascending time order.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, data, order

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_ordered("SELECT val FROM v_own ORDER BY ts ASC;",
                            [(v,) for v in d], "order by ts asc")

    def test_data_order_by_ts_desc(self):
        """Data: ORDER BY ts DESC returns rows in descending time order.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, data, order

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_ordered("SELECT val FROM v_own ORDER BY ts DESC;",
                            [(v,) for v in reversed(d)], "order by ts desc")

    def test_data_limit(self):
        """Data: LIMIT N returns the first N rows.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, data, limit

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        # LIMIT caps returned rows (COUNT(*) is a single-row aggregate unaffected by LIMIT)
        rows = self._rows("SELECT val FROM v_own LIMIT 2;")
        assert len(rows) == 2, f"LIMIT 2 expected 2 rows, got {len(rows)}"
        tdLog.info("  PASS: limit 2 rows")

    def test_data_aggregates(self):
        """Data: COUNT/SUM/AVG/MIN/MAX over the data column.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, data, aggregate

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_count("SELECT COUNT(*) FROM v_own;", len(d), "count all")
        self._check_values("SELECT SUM(val) FROM v_own;", [(sum(d),)], "sum")
        self._check_values("SELECT MIN(val), MAX(val) FROM v_own;", [(min(d), max(d))], "min/max")

    def test_data_group_by_value(self):
        """Data: GROUP BY the data column.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, data, aggregate, group

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values("SELECT val, COUNT(*) FROM v_own GROUP BY val;",
                           [(v, 1) for v in d], "group by val")

    def test_data_combined_filter(self):
        """Data: WHERE data AND time combined filter.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, data, filter

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        # val>=2 AND ts>base: every row except the first (val=1, ts=base) satisfies both
        self._check_values(f"SELECT val FROM v_own WHERE val >= 2 AND ts > {TS0};",
                           [(v,) for v in d if v >= 2], "data AND time")

    # ==================================================================
    # 2. DESC — metadata
    # ==================================================================

    def test_desc_full_schema(self):
        """DESC: lists all columns and tags with correct types.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, metadata, desc

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.query("DESC v_mixed;")
        # ts + val (cols) + lit + rcity (tags) = 4 rows
        tdSql.checkRows(4)
        fields = {str(tdSql.getData(i, 0)): str(tdSql.getData(i, 1)) for i in range(tdSql.queryRows)}
        assert fields.get('ts') == 'TIMESTAMP', f"ts type wrong: {fields}"
        assert fields.get('val') == 'INT', f"val type wrong: {fields}"
        assert fields.get('lit') == 'INT', f"lit type wrong: {fields}"
        assert fields.get('rcity') == 'NCHAR', f"rcity type wrong: {fields}"
        # tags carry the TAG note
        notes = {str(tdSql.getData(i, 0)): str(tdSql.getData(i, 3)) for i in range(tdSql.queryRows)}
        assert notes.get('lit') == 'TAG' and notes.get('rcity') == 'TAG', f"tag notes wrong: {notes}"

    def test_desc_tag_ref_refcolumn(self):
        """DESC: the data column's ref column (val FROM src0.val) is shown.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, metadata, desc, tag_ref

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.query("DESC v_ref;")
        # DESC columns: field | type | length | note | ref ; ref is column index 4
        refs = {str(tdSql.getData(i, 0)): str(tdSql.getData(i, 4)) for i in range(tdSql.queryRows)}
        assert 'src0.val' in refs.get('val', ''), f"col ref not shown: {refs}"

    # ==================================================================
    # 3. SHOW CREATE TABLE
    # ==================================================================

    def test_show_create_owned(self):
        """SHOW CREATE: emits TAGS with the owned inline value.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, metadata, show_create, owned_tag

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.query("SHOW CREATE TABLE v_own;")
        tdSql.checkRows(1)
        sql = str(tdSql.getData(0, 1))
        assert "TAGS" in sql.upper() and "loc" in sql, f"owned TAGS missing: {sql}"

    def test_show_create_tag_ref(self):
        """SHOW CREATE: emits the tag-ref FROM clause.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, metadata, show_create, tag_ref

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.query("SHOW CREATE TABLE v_ref;")
        tdSql.checkRows(1)
        sql = str(tdSql.getData(0, 1))
        assert "TAGS" in sql.upper() and "rcity" in sql and "FROM" in sql.upper(), \
            f"tag-ref missing: {sql}"

    def test_show_create_mixed(self):
        """SHOW CREATE: emits both owned and tag-ref in one TAGS clause.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, metadata, show_create, owned_tag, tag_ref

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.query("SHOW CREATE TABLE v_mixed;")
        tdSql.checkRows(1)
        sql = str(tdSql.getData(0, 1))
        assert "lit" in sql and "rcity" in sql and "TAGS" in sql.upper(), \
            f"mixed TAGS missing: {sql}"

    def test_show_create_roundtrip(self):
        """SHOW CREATE: output re-creates an equivalent table (values survive).

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, metadata, show_create, owned_tag, tag_ref

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE VTABLE vctb_rt (ts TIMESTAMP, val INT FROM src0.val) "
                      "TAGS (loc INT = 9, rcity NCHAR(20) FROM src0.city);")
        tdSql.query("SHOW CREATE TABLE vctb_rt;")
        tdSql.checkRows(1)
        create_sql = str(tdSql.getData(0, 1))
        tdSql.execute("DROP TABLE vctb_rt;")
        tdSql.execute(create_sql)
        self._check_values("SELECT DISTINCT loc FROM vctb_rt;", [(9,)], "round-trip owned")
        self._check_values("SELECT DISTINCT rcity FROM vctb_rt;", [('beijing',)],
                           "round-trip tag-ref")

    def test_show_create_roundtrip_null_owned(self):
        """SHOW CREATE: a NULL owned tag (`= NULL`) round-trips.

        `= NULL` is an explicit value (accepted at CREATE VTABLE). CREATE VTABLE rejects a
        bare `loc INT`, so the emitted DDL must preserve `= NULL` — otherwise replay fails.
        Regression cover for the SHOW CREATE NULL-owned-tag round-trip gap.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, metadata, show_create, owned_tag

        Jira: None

        History:
            - 2026-07-31 Created — covers `= NULL` as an explicit value + round-trip.
        """
        tdSql.execute(f"USE {DB};")
        # `= NULL` is an explicit value: accepted at CREATE, stored as a NULL-valued owned tag.
        tdSql.execute("CREATE VTABLE vctb_null (ts TIMESTAMP, val INT FROM src0.val) "
                      "TAGS (loc INT = NULL);")
        self._check_values("SELECT DISTINCT loc FROM vctb_null;", [(None,)],
                           "vtable: = NULL stored as NULL")
        # SHOW CREATE must emit replayable DDL. CREATE VTABLE rejects a bare `loc INT`, so the
        # output must carry the explicit value (`= NULL`).
        tdSql.query("SHOW CREATE TABLE vctb_null;")
        tdSql.checkRows(1)
        create_sql = str(tdSql.getData(0, 1))
        assert "loc" in create_sql and "= NULL" in create_sql, \
            f"SHOW CREATE must preserve = NULL for the owned tag: {create_sql}"
        tdSql.execute("DROP TABLE vctb_null;")
        tdSql.execute(create_sql)
        self._check_values("SELECT DISTINCT loc FROM vctb_null;", [(None,)],
                           "vtable: = NULL survives round-trip")

    def test_show_create_tagless_omits_tags(self):
        """SHOW CREATE on a tag-less table omits the TAGS clause entirely (no empty `TAGS()`),
        for both normal and virtual-normal tables.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, metadata, show_create

        Jira: None

        History:
            - 2026-07-31 Created — no-tag tables must not emit a TAGS clause.
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE n_notag (ts TIMESTAMP, val INT);")
        tdSql.query("SHOW CREATE TABLE n_notag;")
        sql_n = str(tdSql.getData(0, 1)).upper()
        assert " TAGS (" not in sql_n, f"tag-less normal table must not emit TAGS: {sql_n}"
        tdLog.info("  PASS: tag-less normal table omits TAGS")
        tdSql.execute("CREATE VTABLE v_notag (ts TIMESTAMP, val INT FROM src0.val);")
        tdSql.query("SHOW CREATE TABLE v_notag;")
        sql_v = str(tdSql.getData(0, 1)).upper()
        assert " TAGS (" not in sql_v, f"tag-less virtual table must not emit TAGS: {sql_v}"
        tdLog.info("  PASS: tag-less virtual table omits TAGS")

    # ==================================================================
    # 4. TAG / TAG-REF QUERIES
    # ==================================================================

    def test_tag_owned_value(self):
        """Tag: SELECT an owned INT tag returns the constant value per row.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, owned_tag

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values("SELECT loc FROM v_own;", [(5,) for _ in d], "owned int tag per row")

    def test_tag_owned_nchar_value(self):
        """Tag: SELECT an owned NCHAR tag returns the string value.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, owned_tag

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values("SELECT name FROM v_ntag;", [('alpha',) for _ in d],
                           "owned nchar tag")

    def test_tag_owned_multiple(self):
        """Tag: SELECT multiple owned tags together.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, owned_tag

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values("SELECT name, cnt FROM v_ntag;", [('alpha', 7) for _ in d],
                           "multiple owned tags")

    def test_tag_ref_value(self):
        """Tag: SELECT a tag-ref returns the source tag value.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, tag_ref

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        self._check_values("SELECT DISTINCT rcity, rcode FROM v_ref;",
                           [('beijing', 100)], "tag-ref values")

    def test_tag_ref_follow_source(self):
        """Tag: a tag-ref dynamically follows the source tag after SET TAG.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, tag_ref

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("ALTER TABLE src0 SET TAG city = 'nanjing';")
        try:
            self._check_values("SELECT DISTINCT rcity FROM v_ref;", [('nanjing',)],
                               "tag-ref follows source")
        finally:
            tdSql.execute("ALTER TABLE src0 SET TAG city = 'beijing';")

    def test_tag_mixed_owned_ref(self):
        """Tag: SELECT owned + tag-ref tags together with data.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, owned_tag, tag_ref, mixed

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values("SELECT lit, rcity, val FROM v_mixed;",
                           [(100, 'beijing', v) for v in d], "mixed tags + data")

    def test_tag_multi_source_ref(self):
        """Tag: tag-refs pulling from different source child tables.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, tag_ref, multi_source

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        self._check_values("SELECT DISTINCT c0, c1, c2 FROM v_mref;",
                           [('beijing', 'shanghai', 'guangzhou')], "multi-source tag-refs")

    def test_tag_owned_with_data(self):
        """Tag: owned tag projected together with the data column.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, owned_tag, projection

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values("SELECT loc, val FROM v_own;", [(5, v) for v in d],
                           "owned tag + data")

    def test_tag_owned_filter_eq(self):
        """Tag: WHERE owned_tag = const filters rows.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, owned_tag, filter

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values("SELECT val FROM v_own WHERE loc = 5;", [(v,) for v in d],
                           "owned tag eq filter")

    def test_tag_owned_filter_range(self):
        """Tag: WHERE owned_tag > N / < N filters rows.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, owned_tag, filter

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values("SELECT val FROM v_own WHERE loc > 1;", [(v,) for v in d], "tag > filter")
        self._check_count("SELECT COUNT(*) FROM v_own WHERE loc > 999;", 0, "tag > no-match")

    def test_tag_owned_filter_combined(self):
        """Tag: WHERE owned_tag AND data column combined.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, owned_tag, filter

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values("SELECT val FROM v_own WHERE loc = 5 AND val >= 2;",
                           [(v,) for v in d if v >= 2], "tag AND data filter")

    def test_tag_ref_filter(self):
        """Tag: WHERE on a tag-ref filters rows.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, tag_ref, filter

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values("SELECT val FROM v_ref WHERE rcity = 'beijing';", [(v,) for v in d],
                           "tag-ref eq filter")
        self._check_values("SELECT val FROM v_ref WHERE rcode = 100;", [(v,) for v in d],
                           "tag-ref int eq filter")

    def test_tag_group_by_owned(self):
        """Tag: GROUP BY an owned tag.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, owned_tag, aggregate, group

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values("SELECT loc, COUNT(*), SUM(val) FROM v_own GROUP BY loc;",
                           [(5, len(d), sum(d))], "group by owned tag")

    def test_tag_group_by_tag_ref(self):
        """Tag: GROUP BY a tag-ref.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, tag_ref, aggregate, group

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values("SELECT rcity, COUNT(*), SUM(val) FROM v_ref GROUP BY rcity;",
                           [('beijing', len(d), sum(d))], "group by tag-ref")

    def test_tag_distinct_owned(self):
        """Tag: DISTINCT collapses the constant owned tag value.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, owned_tag, distinct

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        self._check_values("SELECT DISTINCT loc FROM v_own;", [(5,)], "distinct owned tag")

    def test_tag_distinct_tag_ref(self):
        """Tag: DISTINCT collapses tag-ref values.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, tag_ref, distinct

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        self._check_values("SELECT DISTINCT rcity, rcode FROM v_ref;", [('beijing', 100)],
                           "distinct tag-refs")

    def test_tag_agg_with_owned_filter(self):
        """Tag: aggregate with an owned-tag filter.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, owned_tag, aggregate, filter

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        d = SRC['src0']['data']
        self._check_values("SELECT COUNT(*), SUM(val) FROM v_own WHERE loc = 5;",
                           [(len(d), sum(d))], "agg + owned-tag filter")

    # ==================================================================
    # LIFECYCLE: ADD / SET / DROP TAG, ADD TAG ... FROM, drop+recreate
    # ==================================================================

    def test_alter_add_tag(self):
        """Lifecycle: ALTER ADD TAG adds an owned tag (visible in DESC).

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, alter, owned_tag

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE VTABLE vctb_add (ts TIMESTAMP, val INT FROM src0.val);")
        tdSql.execute("ALTER TABLE vctb_add ADD TAG extra INT;")
        tdSql.query("DESC vctb_add;")
        names = {str(tdSql.getData(i, 0)) for i in range(tdSql.queryRows)}
        assert "extra" in names, f"ADD TAG not visible: {names}"

    def test_alter_add_tag_same_conn_select(self):
        """Lifecycle: after ALTER ADD TAG on a persistent connection, the new tag is immediately
        readable (ALTER response carries the full meta — no catalog-cache staleness).

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, alter, owned_tag

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE VTABLE vctb_sameconn (ts TIMESTAMP, val INT FROM src0.val);")
        tdSql.execute("ALTER TABLE vctb_sameconn ADD TAG city INT;")
        tdSql.execute("ALTER TABLE vctb_sameconn SET TAG city = 7;")
        tdSql.query("DESC vctb_sameconn;")
        names = {str(tdSql.getData(i, 0)) for i in range(tdSql.queryRows)}
        assert "city" in names, f"ADD TAG not visible same-connection: {names}"
        self._check_values("SELECT DISTINCT city FROM vctb_sameconn;", [(7,)], "same-conn tag value")

    def test_alter_set_tag(self):
        """Lifecycle: SET TAG updates an owned tag value, readable via SELECT.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, alter, owned_tag

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE VTABLE vctb_set (ts TIMESTAMP, val INT FROM src0.val) "
                      "TAGS (own INT = 0);")
        tdSql.execute("ALTER TABLE vctb_set SET TAG own = 42;")
        self._check_values("SELECT DISTINCT own FROM vctb_set;", [(42,)], "SET TAG value")

    def test_alter_drop_tag(self):
        """Lifecycle: ALTER DROP TAG removes the tag from schema.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, alter, owned_tag

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE VTABLE vctb_drop (ts TIMESTAMP, val INT FROM src0.val) "
                      "TAGS (a INT = 1, b INT = 2);")
        tdSql.execute("ALTER TABLE vctb_drop DROP TAG a;")
        tdSql.query("DESC vctb_drop;")
        names = {str(tdSql.getData(i, 0)) for i in range(tdSql.queryRows)}
        assert "a" not in names and "b" in names, f"DROP TAG wrong: {names}"

    def test_alter_add_tag_ref(self):
        """Lifecycle: ALTER ADD TAG name TYPE FROM src.tag adds a tag reference.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, alter, tag_ref

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE VTABLE vctb_addref (ts TIMESTAMP, val INT FROM src0.val);")
        tdSql.execute("ALTER TABLE vctb_addref ADD TAG tcity NCHAR(20) FROM src0.city;")
        self._check_values("SELECT DISTINCT tcity FROM vctb_addref;", [('beijing',)],
                           "ADD TAG ... FROM value")

    def test_drop_recreate(self):
        """Lifecycle: drop and recreate with different tags.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, drop, recreate

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE VTABLE vctb_dr (ts TIMESTAMP, val INT FROM src0.val) TAGS (x INT = 1);")
        tdSql.execute("DROP TABLE vctb_dr;")
        tdSql.error("SHOW CREATE TABLE vctb_dr;")
        tdSql.execute("CREATE VTABLE vctb_dr (ts TIMESTAMP, val INT FROM src0.val) TAGS (y INT = 2);")
        self._check_values("SELECT DISTINCT y FROM vctb_dr;", [(2,)], "recreated tag value")

    # ==================================================================
    # ERROR CASES
    # ==================================================================

    def test_error_decimal_rejected(self):
        """Error: DECIMAL tags are rejected at CREATE.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, create, negative

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.error("CREATE VTABLE vctb_e0 (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (d DECIMAL(10,2) = 1);")

    def test_error_dup_tag_name(self):
        """Error: duplicate tag names within TAGS(...) are rejected.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, create, negative

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.error("CREATE VTABLE vctb_e1 (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (a INT = 1, a BIGINT = 2);")

    def test_error_tag_col_collision(self):
        """Error: a tag name colliding with a column name is rejected.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, create, negative

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.error("CREATE VTABLE vctb_e2 (ts TIMESTAMP, dup INT) TAGS (dup INT = 1);")

    def test_error_alter_tag_col_collision(self):
        """Error: ALTER ADD TAG whose name collides with a column is rejected (tags/columns share name space).

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, alter, negative

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE VTABLE vctb_e_altercol (ts TIMESTAMP, val INT FROM src0.val);")
        tdSql.error("ALTER TABLE vctb_e_altercol ADD TAG val INT;")  # 'val' is a column

    def test_error_json_tag_mixed(self):
        """Error: a JSON tag coexisting with other tags is rejected (a JSON tag is the whole
        payload; mixing would silently lose the other tag values).

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, create, negative

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.error("CREATE VTABLE vctb_e_jsonmix (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (j JSON, n INT);")

    def test_error_nonexistent_ref_tag(self):
        """Error: tag-ref pointing to a non-existent source tag is rejected.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, create, tag_ref, negative

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.error("CREATE VTABLE vctb_e3 (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (r NCHAR(20) FROM src0.nonexistent);")

    def test_error_ref_data_column(self):
        """Error: tag-ref must reference a tag column, not a data column.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, create, tag_ref, negative

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.error("CREATE VTABLE vctb_e4 (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (r INT FROM src0.val);")

    # ==================================================================
    # 5. PLAIN NORMAL TABLE owned tags — ADD/SET/DROP TAG, projection,
    #    filter, GROUP BY/DISTINCT, DESC, SHOW CREATE, error cases.
    #    (normal tables support owned tags only; tag-ref FROM stays
    #    virtual-normal-table only)
    # ==================================================================

    def test_ntb_add_set_query(self):
        """Normal table: ADD TAG + SET TAG, projection and WHERE filter on owned tags.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, alter, select

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE ntb_p1 (ts TIMESTAMP, val INT);")
        for j, v in enumerate([1, 2, 3]):
            tdSql.execute(f"INSERT INTO ntb_p1 VALUES ({TS0 + j * 1000}, {v});")
        tdSql.execute("ALTER TABLE ntb_p1 ADD TAG loc INT;")
        tdSql.execute("ALTER TABLE ntb_p1 ADD TAG city VARCHAR(16);")
        # new tags default to NULL
        self._check_values("SELECT loc, city FROM ntb_p1;",
                           [(None, None), (None, None), (None, None)],
                           "ntb: new tags default NULL")
        tdSql.execute("ALTER TABLE ntb_p1 SET TAG loc = 5;")
        tdSql.execute("ALTER TABLE ntb_p1 SET TAG city = 'beijing';")
        self._check_values("SELECT loc, city, val FROM ntb_p1;",
                           [(5, 'beijing', 1), (5, 'beijing', 2), (5, 'beijing', 3)],
                           "ntb: owned tag projection")
        self._check_values("SELECT val FROM ntb_p1 WHERE loc = 5;", [(1,), (2,), (3,)],
                           "ntb: tag filter hit")
        self._check_values("SELECT val FROM ntb_p1 WHERE loc = 6;", [],
                           "ntb: tag filter miss")
        self._check_values("SELECT val FROM ntb_p1 WHERE city = 'beijing' AND loc >= 5;",
                           [(1,), (2,), (3,)],
                           "ntb: combined tag filter")
        # SET TAG NULL clears the value
        tdSql.execute("ALTER TABLE ntb_p1 SET TAG loc = NULL;")
        self._check_values("SELECT DISTINCT loc FROM ntb_p1;", [(None,)],
                           "ntb: SET TAG NULL clears value")

    def test_ntb_alter_drop_tag(self):
        """Normal table: DROP TAG removes the tag; same connection sees it immediately.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, alter

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE ntb_p2 (ts TIMESTAMP, val INT);")
        tdSql.execute(f"INSERT INTO ntb_p2 VALUES ({TS0}, 1);")
        tdSql.execute("ALTER TABLE ntb_p2 ADD TAG loc INT;")
        tdSql.execute("ALTER TABLE ntb_p2 SET TAG loc = 7;")
        self._check_values("SELECT loc, val FROM ntb_p2;", [(7, 1)],
                           "ntb: same-conn read after ADD TAG")
        tdSql.execute("ALTER TABLE ntb_p2 DROP TAG loc;")
        rows = self._rows("DESC ntb_p2;")
        names = sorted(r[0] for r in rows)
        assert names == sorted(['ts', 'val']), f"DESC ntb_p2 fields after DROP TAG: {names}"
        tdLog.info("  PASS: ntb reverts to tag-less after dropping the last tag")
        self._check_values("SELECT val FROM ntb_p2;", [(1,)],
                           "ntb: data intact after DROP TAG")

    def test_ntb_desc_show_create(self):
        """Normal table: DESC marks owned tags; SHOW CREATE emits the table's FINAL form —
        a single CREATE TABLE with an inline TAGS clause (NULL tag -> `= NULL`, valued tag
        -> `= value`), not a CREATE + ALTER ADD/SET TAG sequence.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, desc, show_create

        Jira: None

        History:
            - 2026-07-27 Created
            - 2026-07-31 Updated — SHOW CREATE shows the final inline TAGS form, not ALTER stmts.
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE ntb_p3 (ts TIMESTAMP, val INT);")
        tdSql.execute("ALTER TABLE ntb_p3 ADD TAG loc INT;")
        tdSql.execute("ALTER TABLE ntb_p3 ADD TAG owner VARCHAR(16);")
        tdSql.execute("ALTER TABLE ntb_p3 SET TAG owner = 'alice';")

        rows = self._rows("DESC ntb_p3;")
        tag_rows = [r for r in rows if r[3] == 'TAG']
        tag_names = sorted(r[0] for r in tag_rows)
        assert tag_names == ['loc', 'owner'], f"DESC ntb_p3 tag rows: {tag_names}"
        tdLog.info("  PASS: DESC marks owned tags")

        # SHOW CREATE emits the final table form: one CREATE TABLE with an inline TAGS clause,
        # NOT a CREATE + ALTER ADD TAG / SET TAG sequence. NULL renders inline as `= NULL`.
        tdSql.query("SHOW CREATE TABLE ntb_p3;")
        create_sql = str(tdSql.getData(0, 1))
        assert create_sql.startswith("CREATE TABLE `ntb_p3`"), f"SHOW CREATE ntb_p3: {create_sql}"
        assert "TAGS" in create_sql.upper(), f"inline TAGS clause missing: {create_sql}"
        assert "`loc`" in create_sql and "`owner`" in create_sql, f"tags missing: {create_sql}"
        assert "= NULL" in create_sql, f"NULL tag loc must render as = NULL: {create_sql}"
        assert '= "alice"' in create_sql, f"owner value must render inline: {create_sql}"
        assert "ADD TAG" not in create_sql and "SET TAG" not in create_sql, \
            f"SHOW CREATE must show the final table, not an ALTER sequence: {create_sql}"
        tdLog.info(f"  PASS: SHOW CREATE emits final inline TAGS form: {create_sql}")

        # round-trip: the single inline CREATE reproduces the table with identical tags.
        tdSql.execute("DROP TABLE ntb_p3;")
        tdSql.execute(create_sql)
        rows = self._rows("DESC ntb_p3;")
        tag_names = sorted(r[0] for r in rows if r[3] == 'TAG')
        assert tag_names == ['loc', 'owner'], f"round-trip DESC ntb_p3 tag rows: {tag_names}"
        # the recreated table is empty; insert a row to read back the replayed tag value
        tdSql.execute(f"INSERT INTO ntb_p3 VALUES ({TS0}, 1);")
        self._check_values("SELECT loc, owner, val FROM ntb_p3;", [(None, 'alice', 1)],
                           "round-trip owned tag value")
        tdLog.info("  PASS: SHOW CREATE round-trip restores tags")

    def test_ntb_create_tag_explicit_null_roundtrip(self):
        """Normal table: SHOW CREATE emits the table's FINAL form — a single CREATE TABLE
        with an inline TAGS clause, not a CREATE + ALTER ADD/SET TAG sequence. A NULL-valued
        owned tag renders inline as `= NULL` (a valid explicit value), which round-trips.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, show_create

        Jira: None

        History:
            - 2026-07-31 Created — `= NULL` explicit value + final-form SHOW CREATE.
        """
        tdSql.execute(f"USE {DB};")
        # every tag carries `= literal` (= NULL is a literal) -> normal table with owned tags,
        # not a super table (which requires every tag valueless).
        tdSql.execute("CREATE TABLE ntb_null (ts TIMESTAMP, val INT) TAGS (loc INT = NULL);")
        # SHOW CREATE must show the final table: one CREATE TABLE with an inline TAGS clause,
        # NOT a CREATE + ALTER ADD TAG / SET TAG sequence. NULL renders inline as `= NULL`.
        tdSql.query("SHOW CREATE TABLE ntb_null;")
        create_sql = str(tdSql.getData(0, 1))
        assert create_sql.startswith("CREATE TABLE `ntb_null`"), \
            f"= NULL must create a normal table, not a stable: {create_sql}"
        assert "TAGS" in create_sql.upper() and "`loc`" in create_sql, \
            f"SHOW CREATE must emit an inline TAGS clause: {create_sql}"
        assert "= NULL" in create_sql, \
            f"NULL owned tag must render inline as = NULL: {create_sql}"
        assert "ADD TAG" not in create_sql and "SET TAG" not in create_sql, \
            f"SHOW CREATE must show the final table, not an ALTER sequence: {create_sql}"
        # round-trip: the single inline CREATE reproduces the table with the NULL tag.
        tdSql.execute("DROP TABLE ntb_null;")
        tdSql.execute(create_sql)
        rows = self._rows("DESC ntb_null;")
        tag_names = sorted(r[0] for r in rows if r[3] == 'TAG')
        assert tag_names == ['loc'], f"round-trip DESC ntb_null tag rows: {tag_names}"
        tdSql.execute(f"INSERT INTO ntb_null VALUES ({TS0}, 1);")
        self._check_values("SELECT loc FROM ntb_null;", [(None,)],
                           "ntb: = NULL survives round-trip")

    def test_ntb_group_distinct_agg(self):
        """Normal table: GROUP BY / DISTINCT on owned tags, agg with tag filter.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, group_by, aggregate

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE ntb_p4 (ts TIMESTAMP, val INT);")
        for j, v in enumerate([1, 2, 3, 4]):
            tdSql.execute(f"INSERT INTO ntb_p4 VALUES ({TS0 + j * 1000}, {v});")
        tdSql.execute("ALTER TABLE ntb_p4 ADD TAG grp VARCHAR(8);")
        tdSql.execute("ALTER TABLE ntb_p4 SET TAG grp = 'g1';")
        self._check_values("SELECT grp, COUNT(*) FROM ntb_p4 GROUP BY grp;", [('g1', 4)],
                           "ntb: GROUP BY tag")
        self._check_values("SELECT DISTINCT grp FROM ntb_p4;", [('g1',)],
                           "ntb: DISTINCT tag")
        self._check_values("SELECT SUM(val) FROM ntb_p4 WHERE grp = 'g1';", [(10,)],
                           "ntb: agg with tag filter")
        # SELECT * excludes tags (plain-column semantics unchanged)
        tdSql.query("SELECT * FROM ntb_p4 LIMIT 1;")
        assert tdSql.queryCols == 2, f"SELECT * column count: {tdSql.queryCols}"
        tdLog.info("  PASS: SELECT * excludes tags")

    def test_ntb_error_cases(self):
        """Normal table: dup tag, tag/col collision, decimal, tag-ref FROM, bad SET/DROP.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, alter, negative

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE ntb_e1 (ts TIMESTAMP, val INT);")
        tdSql.execute("ALTER TABLE ntb_e1 ADD TAG loc INT;")
        tdSql.error("ALTER TABLE ntb_e1 ADD TAG loc INT;")          # duplicate tag name
        tdSql.error("ALTER TABLE ntb_e1 ADD TAG val INT;")          # collides with column
        tdSql.error("ALTER TABLE ntb_e1 ADD TAG dc DECIMAL(10,2);")  # decimal not allowed
        # tag-ref FROM stays virtual-normal-table only
        tdSql.error("ALTER TABLE ntb_e1 ADD TAG r NCHAR(20) FROM src0.city;")
        tdSql.error("ALTER TABLE ntb_e1 DROP TAG nosuch;")          # drop nonexistent tag
        tdSql.error("ALTER TABLE ntb_e1 SET TAG nosuch = 5;")       # set nonexistent tag
        # tagless normal table rejects SET/DROP TAG as before
        tdSql.execute("CREATE TABLE ntb_e2 (ts TIMESTAMP, val INT);")
        tdSql.error("ALTER TABLE ntb_e2 SET TAG loc = 5;")
        tdSql.error("ALTER TABLE ntb_e2 DROP TAG loc;")
        rows = self._rows("DESC ntb_e2;")
        names = sorted(r[0] for r in rows)
        assert names == sorted(['ts', 'val']), f"DESC ntb_e2 fields: {names}"
        tdLog.info("  PASS: tagless normal table stays tag-less")

    # ==================================================================
    # 6. PLAIN NORMAL TABLE drop/re-add scenarios — same-name re-add,
    #    type change, position variants, cycles, catalog freshness
    # ==================================================================

    def test_ntb_drop_readd_same_name(self):
        """Normal table: DROP TAG then ADD TAG with the same name — old value must not
        resurrect, and the same connection must see the change immediately.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, alter, drop_add

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE ntb_d1 (ts TIMESTAMP, val INT);")
        for j, v in enumerate([1, 2]):
            tdSql.execute(f"INSERT INTO ntb_d1 VALUES ({TS0 + j * 1000}, {v});")
        tdSql.execute("ALTER TABLE ntb_d1 ADD TAG a INT;")
        tdSql.execute("ALTER TABLE ntb_d1 SET TAG a = 42;")
        self._check_values("SELECT a, val FROM ntb_d1;", [(42, 1), (42, 2)],
                           "ntb: tag set before drop")
        tdSql.execute("ALTER TABLE ntb_d1 DROP TAG a;")
        # same connection must accept the re-add immediately (catalog freshness)
        tdSql.execute("ALTER TABLE ntb_d1 ADD TAG a INT;")
        self._check_values("SELECT a, val FROM ntb_d1;", [(None, 1), (None, 2)],
                           "ntb: re-added tag is NULL — old value must not resurrect")
        tdSql.execute("ALTER TABLE ntb_d1 SET TAG a = 7;")
        self._check_values("SELECT a, val FROM ntb_d1 WHERE a = 7;", [(7, 1), (7, 2)],
                           "ntb: re-added tag set + filter")

    def test_ntb_drop_readd_diff_type(self):
        """Normal table: DROP TAG then re-add the same name with a different type/length.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, alter, drop_add

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE ntb_d2 (ts TIMESTAMP, val INT);")
        tdSql.execute(f"INSERT INTO ntb_d2 VALUES ({TS0}, 1);")
        tdSql.execute("ALTER TABLE ntb_d2 ADD TAG a INT;")
        tdSql.execute("ALTER TABLE ntb_d2 SET TAG a = 42;")
        tdSql.execute("ALTER TABLE ntb_d2 DROP TAG a;")
        # re-add same name as VARCHAR
        tdSql.execute("ALTER TABLE ntb_d2 ADD TAG a VARCHAR(16);")
        tdSql.execute("ALTER TABLE ntb_d2 SET TAG a = 'x';")
        self._check_values("SELECT a, val FROM ntb_d2 WHERE a = 'x';", [('x', 1)],
                           "ntb: re-added tag with new type")
        # re-add again with a different length
        tdSql.execute("ALTER TABLE ntb_d2 DROP TAG a;")
        tdSql.execute("ALTER TABLE ntb_d2 ADD TAG a VARCHAR(64);")
        rows = self._rows("DESC ntb_d2;")
        a_row = [r for r in rows if r[0] == 'a']
        assert a_row and a_row[0][1] == 'VARCHAR' and a_row[0][2] == '64', \
            f"DESC ntb_d2 tag a: {a_row}"
        self._check_values("SELECT a FROM ntb_d2;", [(None,)],
                           "ntb: re-added tag defaults NULL after type change")

    def test_ntb_drop_position_variants(self):
        """Normal table: drop first / middle / last tag; survivors keep their values.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, alter, drop_add

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE ntb_d3 (ts TIMESTAMP, val INT);")
        tdSql.execute(f"INSERT INTO ntb_d3 VALUES ({TS0}, 1);")
        tdSql.execute("ALTER TABLE ntb_d3 ADD TAG t1 INT;")
        tdSql.execute("ALTER TABLE ntb_d3 ADD TAG t2 VARCHAR(8);")
        tdSql.execute("ALTER TABLE ntb_d3 ADD TAG t3 BIGINT;")
        tdSql.execute("ALTER TABLE ntb_d3 SET TAG t1 = 1;")
        tdSql.execute("ALTER TABLE ntb_d3 SET TAG t2 = 'two';")
        tdSql.execute("ALTER TABLE ntb_d3 SET TAG t3 = 3;")
        # drop the middle tag: survivors keep values
        tdSql.execute("ALTER TABLE ntb_d3 DROP TAG t2;")
        self._check_values("SELECT t1, t3, val FROM ntb_d3;", [(1, 3, 1)],
                           "ntb: values intact after dropping the middle tag")
        rows = self._rows("DESC ntb_d3;")
        tag_names = [r[0] for r in rows if r[3] == 'TAG']
        assert tag_names == ['t1', 't3'], f"DESC ntb_d3 after middle drop: {tag_names}"
        # re-add the dropped name: goes last, defaults NULL
        tdSql.execute("ALTER TABLE ntb_d3 ADD TAG t2 VARCHAR(8);")
        self._check_values("SELECT t1, t2, t3 FROM ntb_d3;", [(1, None, 3)],
                           "ntb: re-added middle tag defaults NULL, survivors intact")
        # drop the first and the last tag
        tdSql.execute("ALTER TABLE ntb_d3 DROP TAG t1;")
        self._check_values("SELECT t3 FROM ntb_d3;", [(3,)],
                           "ntb: value intact after dropping the first tag")
        tdSql.execute("ALTER TABLE ntb_d3 DROP TAG t3;")
        self._check_values("SELECT t2 FROM ntb_d3;", [(None,)],
                           "ntb: only the re-added tag remains")
        # drop the last remaining tag: table reverts to tag-less
        tdSql.execute("ALTER TABLE ntb_d3 DROP TAG t2;")
        rows = self._rows("DESC ntb_d3;")
        assert sorted(r[0] for r in rows) == sorted(['ts', 'val']), \
            f"DESC ntb_d3 tagless: {sorted(r[0] for r in rows)}"
        tdLog.info("  PASS: drop position variants")

    def test_ntb_add_drop_cycles(self):
        """Normal table: repeated ADD/DROP TAG cycles on the same name stay consistent.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, alter, drop_add

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE ntb_d4 (ts TIMESTAMP, val INT);")
        tdSql.execute(f"INSERT INTO ntb_d4 VALUES ({TS0}, 1);")
        for cycle in range(3):
            tdSql.execute("ALTER TABLE ntb_d4 ADD TAG a INT;")
            self._check_values("SELECT a FROM ntb_d4;", [(None,)],
                               f"ntb: cycle {cycle}: re-added tag defaults NULL")
            tdSql.execute(f"ALTER TABLE ntb_d4 SET TAG a = {cycle};")
            self._check_values("SELECT a FROM ntb_d4 WHERE a >= 0;", [(cycle,)],
                               f"ntb: cycle {cycle}: set value")
            tdSql.execute("ALTER TABLE ntb_d4 DROP TAG a;")
            rows = self._rows("DESC ntb_d4;")
            assert sorted(r[0] for r in rows) == sorted(['ts', 'val']), \
                f"ntb: cycle {cycle}: tagless after drop"
        # data rows survive all cycles
        self._check_values("SELECT val FROM ntb_d4;", [(1,)], "ntb: data intact after cycles")

    def test_ntb_drop_negative(self):
        """Normal table: drop twice, select a dropped tag, stale SET on a dropped tag.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, alter, negative

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE ntb_d5 (ts TIMESTAMP, val INT);")
        tdSql.execute(f"INSERT INTO ntb_d5 VALUES ({TS0}, 1);")
        tdSql.execute("ALTER TABLE ntb_d5 ADD TAG a INT;")
        tdSql.execute("ALTER TABLE ntb_d5 SET TAG a = 42;")
        tdSql.execute("ALTER TABLE ntb_d5 DROP TAG a;")
        tdSql.error("ALTER TABLE ntb_d5 DROP TAG a;")       # drop the same tag twice
        tdSql.error("ALTER TABLE ntb_d5 SET TAG a = 1;")    # SET on a dropped tag
        tdSql.error("SELECT a FROM ntb_d5;")                # select a dropped tag
        tdSql.error("SELECT val FROM ntb_d5 WHERE a = 42;")  # filter on a dropped tag
        # data queries still work
        self._check_values("SELECT val FROM ntb_d5;", [(1,)], "ntb: data intact after drop")

    def test_ntb_max_tags_cap(self):
        """Normal table: tag count cap (TSDB_MAX_TAGS=128); dropping one frees a slot.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, alter, boundary

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE ntb_d6 (ts TIMESTAMP, val INT);")
        tdSql.execute(f"INSERT INTO ntb_d6 VALUES ({TS0}, 1);")
        for i in range(128):
            tdSql.execute(f"ALTER TABLE ntb_d6 ADD TAG tg{i} INT;")
        tdSql.error("ALTER TABLE ntb_d6 ADD TAG tg128 INT;")  # 129th tag exceeds the cap
        # dropping one frees a slot for another tag
        tdSql.execute("ALTER TABLE ntb_d6 DROP TAG tg0;")
        tdSql.execute("ALTER TABLE ntb_d6 ADD TAG tg128 INT;")
        tdSql.execute("ALTER TABLE ntb_d6 SET TAG tg128 = 128;")
        self._check_values("SELECT tg128, val FROM ntb_d6;", [(128, 1)],
                           "ntb: 128 tags after drop+add, value readable")
        rows = self._rows("DESC ntb_d6;")
        n_tags = len([r for r in rows if r[3] == 'TAG'])
        assert n_tags == 128, f"DESC ntb_d6 tag count: {n_tags}"
        tdSql.execute("DROP TABLE ntb_d6;")
        tdLog.info("  PASS: max tags cap")

    # ==================================================================
    # 7. VIRTUAL NORMAL TABLE tag-ref <-> tag-literal conversion via
    #    SET TAG — ref->other-ref, ref->literal, literal->ref, cycles
    # ==================================================================

    def test_vtag_conv_ref_to_ref(self):
        """Virtual normal table: SET TAG re-points a tag-ref to another source tag.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, tag_ref, alter, convert

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE VTABLE vc1 (ts TIMESTAMP, val INT FROM src0.val) "
                      "TAGS (rc INT FROM src0.code);")
        self._check_values("SELECT DISTINCT rc FROM vc1;", [(100,)],
                           "conv: initial ref follows src0.code")
        tdSql.execute("ALTER TABLE vc1 SET TAG rc = src1.code;")
        self._check_values("SELECT DISTINCT rc FROM vc1;", [(200,)],
                           "conv: ref re-pointed to src1.code")
        # same connection sees the change; SHOW CREATE reflects the new ref
        tdSql.query("SHOW CREATE TABLE vc1;")
        sql = str(tdSql.getData(0, 1))
        assert "FROM" in sql.upper() and "src1" in sql and "code" in sql, \
            f"SHOW CREATE vc1 after re-point: {sql}"
        tdLog.info("  PASS: ref -> other ref")

    def test_vtag_conv_ref_to_literal(self):
        """Virtual normal table: SET TAG <ref> = value clears the ref (ref -> literal).

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, tag_ref, alter, convert

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE VTABLE vc2 (ts TIMESTAMP, val INT FROM src0.val) "
                      "TAGS (rc INT FROM src0.code);")
        self._check_values("SELECT DISTINCT rc FROM vc2;", [(100,)],
                           "conv: initial ref value")
        tdSql.execute("ALTER TABLE vc2 SET TAG rc = 555;")
        self._check_values("SELECT DISTINCT rc FROM vc2;", [(555,)],
                           "conv: ref converted to literal, same conn")
        tdSql.query("SHOW CREATE TABLE vc2;")
        sql = str(tdSql.getData(0, 1))
        assert "`rc` INT = 555" in sql and "FROM" not in sql.split("TAGS")[1], \
            f"SHOW CREATE vc2 after ref->literal: {sql}"
        tdLog.info("  PASS: ref -> literal, ref cleared in SHOW CREATE")
        # SET NULL on a ref also clears the ref; value becomes NULL
        tdSql.execute("ALTER TABLE vc2 SET TAG rc = src0.code;")
        tdSql.execute("ALTER TABLE vc2 SET TAG rc = NULL;")
        self._check_values("SELECT DISTINCT rc FROM vc2;", [(None,)],
                           "conv: SET NULL on ref clears ref, value NULL")
        tdSql.query("SHOW CREATE TABLE vc2;")
        sql = str(tdSql.getData(0, 1))
        assert "FROM" not in sql.split("TAGS")[1], f"SHOW CREATE vc2 after SET NULL: {sql}"
        tdLog.info("  PASS: SET NULL on ref clears the ref")

    def test_vtag_conv_literal_to_ref(self):
        """Virtual normal table: SET TAG <owned> = db.tb.tag turns an owned tag into a ref.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, tag_ref, alter, convert

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE VTABLE vc3 (ts TIMESTAMP, val INT FROM src0.val) TAGS (lit INT = 0);")
        tdSql.execute("ALTER TABLE vc3 SET TAG lit = 42;")
        self._check_values("SELECT DISTINCT lit FROM vc3;", [(42,)],
                           "conv: owned literal value")
        tdSql.execute("ALTER TABLE vc3 SET TAG lit = src1.code;")
        self._check_values("SELECT DISTINCT lit FROM vc3;", [(200,)],
                           "conv: literal converted to ref, follows src1.code")
        # the static value must not resurrect after converting back and forth
        tdSql.query("SHOW CREATE TABLE vc3;")
        sql = str(tdSql.getData(0, 1))
        assert "FROM" in sql.upper() and "src1" in sql, f"SHOW CREATE vc3 literal->ref: {sql}"
        tdLog.info("  PASS: literal -> ref")
        # negative: ref must point at a tag, not a data column
        tdSql.error("ALTER TABLE vc3 SET TAG lit = src0.val;")
        # negative: type mismatch (INT tag vs NCHAR source tag)
        tdSql.error("ALTER TABLE vc3 SET TAG lit = src0.city;")
        # negative: physical normal table has no refs
        tdSql.execute("CREATE TABLE vc3_ntb (ts TIMESTAMP, val INT);")
        tdSql.execute("ALTER TABLE vc3_ntb ADD TAG x INT;")
        tdSql.error("ALTER TABLE vc3_ntb SET TAG x = src0.code;")
        tdLog.info("  PASS: ref target validation + ntb ref rejected")

    def test_vtag_conv_cycles(self):
        """Virtual normal table: literal -> ref -> literal -> ref cycles stay consistent.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, tag_ref, alter, convert

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE VTABLE vc4 (ts TIMESTAMP, val INT FROM src0.val) TAGS (t INT = 0);")
        for i in range(2):
            tdSql.execute("ALTER TABLE vc4 SET TAG t = 42;")
            self._check_values("SELECT DISTINCT t FROM vc4;", [(42,)],
                               f"conv cycle {i}: literal 42")
            tdSql.execute("ALTER TABLE vc4 SET TAG t = src1.code;")
            self._check_values("SELECT DISTINCT t FROM vc4;", [(200,)],
                               f"conv cycle {i}: ref src1.code")
            tdSql.execute("ALTER TABLE vc4 SET TAG t = 7;")
            self._check_values("SELECT DISTINCT t FROM vc4;", [(7,)],
                               f"conv cycle {i}: back to literal 7")
            tdSql.execute("ALTER TABLE vc4 SET TAG t = src2.code;")
            self._check_values("SELECT DISTINCT t FROM vc4;", [(300,)],
                               f"conv cycle {i}: ref src2.code")
        tdLog.info("  PASS: literal/ref conversion cycles")

    def test_create_tag_value_required(self):
        """Create rules: CREATE VTABLE tags must carry an explicit value — `= literal`
        (owned) or `FROM db.tb.tag` (tag-ref). A bare `name TYPE` tag is rejected, as is a
        mix of FROM and valueless tags; literal + FROM mixed stays legal.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, create, tag, negative

        Jira: None

        History:
            - 2026-07-29 Created
        """
        tdSql.execute(f"USE {DB};")
        # valueless owned tag rejected
        tdSql.error("CREATE VTABLE vctb_noval (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (t INT);")
        # mix of FROM and valueless rejected
        tdSql.error("CREATE VTABLE vctb_noval2 (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (r NCHAR(20) FROM src0.city, t INT);")
        # literal + FROM mixed is legal
        tdSql.execute("CREATE VTABLE vctb_ok (ts TIMESTAMP, val INT FROM src0.val) "
                      "TAGS (t INT = 1, r NCHAR(20) FROM src0.city);")
        self._check_values("SELECT DISTINCT t FROM vctb_ok;", [(1,)], "mixed literal+FROM owned value")
        # literal value must fit the tag type definition
        tdSql.error("CREATE VTABLE vctb_toolong (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (a VARCHAR(4) = 'abcdefgh');")
        tdSql.error("CREATE VTABLE vctb_overflow (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (a TINYINT = 1000);")
        tdSql.error("CREATE VTABLE vctb_badtype (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (a INT = 'xyz');")
        tdLog.info("  PASS: CREATE VTABLE requires explicit tag values")

    def test_ntb_create_with_literal_tags(self):
        """Normal table: CREATE TABLE ... TAGS where every tag carries `= literal` creates a
        normal table with owned tags, while valueless TAGS keeps the historical super-table
        semantics — the inline values are how CREATE TABLE tells the two apart.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: normal_table, create, tag

        Jira: None

        History:
            - 2026-07-29 Created
        """
        tdSql.execute(f"USE {DB};")
        # all tags valued -> normal table with owned tags
        tdSql.execute("CREATE TABLE ntb_tags (ts TIMESTAMP, val INT) "
                      "TAGS (a INT = 1, b VARCHAR(8) = 'x');")
        tdSql.execute("INSERT INTO ntb_tags VALUES (1700000000000, 10);")
        self._check_values("SELECT a, b FROM ntb_tags;", [(1, 'x')],
                           "ntb create-time owned tag values")
        # SHOW CREATE emits the table's final form: an inline TAGS clause (not an ALTER seq)
        tdSql.query("SHOW CREATE TABLE ntb_tags;")
        sql = str(tdSql.getData(0, 1))
        assert "TAGS" in sql.upper() and "ADD TAG" not in sql.upper(), \
            f"ntb SHOW CREATE must be inline TAGS form: {sql}"
        # it is a normal table: cannot act as a super table
        tdSql.error("CREATE TABLE ntb_tags_child (ts TIMESTAMP, val INT) USING ntb_tags TAGS (2, 'y');")
        # valueless TAGS -> super table (historical semantics)
        tdSql.execute("CREATE TABLE stb_tags (ts TIMESTAMP, val INT) TAGS (a INT);")
        tdSql.execute("CREATE TABLE stb_tags_child USING stb_tags TAGS (1);")
        # mixed valued/valueless -> rejected
        tdSql.error("CREATE TABLE ntb_mixed (ts TIMESTAMP, val INT) TAGS (a INT = 1, b INT);")
        # tag-ref (FROM) stays virtual-normal-table only
        tdSql.error("CREATE TABLE ntb_ref (ts TIMESTAMP, val INT) TAGS (a INT FROM src0.code);")
        # CREATE STABLE never becomes a normal table, even with valued tags
        tdSql.error("CREATE STABLE stb_val (ts TIMESTAMP, val INT) TAGS (a INT = 1);")
        # the inline-value guard also fires on the BASE ON (inherited super-table) path,
        # ahead of BASE ON parent resolution — a valued tag never slips into the STB path
        tdSql.error("CREATE STABLE stb_inh (ts TIMESTAMP, val INT) TAGS (a INT = 1) BASE ON stb_tags;")
        # literal value must fit the tag type definition
        tdSql.error("CREATE TABLE ntb_toolong (ts TIMESTAMP, val INT) TAGS (a VARCHAR(4) = 'abcdefgh');")
        tdSql.error("CREATE TABLE ntb_nctoolong (ts TIMESTAMP, val INT) TAGS (a NCHAR(2) = '汉字汉');")
        tdSql.error("CREATE TABLE ntb_overflow (ts TIMESTAMP, val INT) TAGS (a TINYINT = 1000);")
        tdSql.error("CREATE TABLE ntb_badtype (ts TIMESTAMP, val INT) TAGS (a INT = 'xyz');")
        # type/value validation matches child-table tag semantics (shared parseTagValue):
        # compatible literals convert, out-of-range/invalid literals are rejected
        tdSql.execute("CREATE TABLE ntb_conv (ts TIMESTAMP, val INT) TAGS (a INT = 1.5, b VARCHAR(8) = 123);")
        tdSql.execute("INSERT INTO ntb_conv VALUES (1700000000000, 1);")
        self._check_values("SELECT a, b FROM ntb_conv;", [(2, '123')],
                           "create-time literal conversion per tag semantics")
        tdSql.execute("CREATE STABLE stb_conv (ts TIMESTAMP, val INT) TAGS (a INT, b VARCHAR(8));")
        tdSql.execute("CREATE TABLE stb_conv_c1 USING stb_conv TAGS (1.5, 123);")
        tdSql.execute("INSERT INTO stb_conv_c1 VALUES (1700000000000, 1);")
        self._check_values("SELECT a, b FROM stb_conv_c1;", [(2, '123')],
                           "child-table tag conversion (control, same semantics)")
        tdLog.info("  PASS: CREATE TABLE tag value rules")

    def test_ins_tags_and_show_tags(self):
        """ins_tags and SHOW TAGS expose owned tags and tag-refs of normal and
        virtual normal tables; tag-less tables produce no rows.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, tag, ins_tags, show

        Jira: None

        History:
            - 2026-07-31 Created
        """
        tdSql.execute(f"USE {DB};")

        # vntb owned tag (fixture v_own: TAGS (loc INT = 5))
        tdSql.query("SELECT tag_name, tag_value FROM information_schema.ins_tags "
                    f"WHERE db_name='{DB}' AND table_name='v_own';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'loc')
        tdSql.checkData(0, 1, '5')

        # stable_name is NULL for normal/virtual-normal tables
        tdSql.query("SELECT stable_name FROM information_schema.ins_tags "
                    f"WHERE db_name='{DB}' AND table_name='v_own';")
        tdSql.checkData(0, 0, None)

        # vntb tag-refs resolve to the source table values (v_ref: rcity/rcode from src0)
        tdSql.query("SELECT tag_name, tag_value FROM information_schema.ins_tags "
                    f"WHERE db_name='{DB}' AND table_name='v_ref' ORDER BY tag_name;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'rcity')
        tdSql.checkData(0, 1, 'beijing')
        tdSql.checkData(1, 0, 'rcode')
        tdSql.checkData(1, 1, '100')

        # physical normal table owned tags
        tdSql.execute("CREATE TABLE it_ntb (ts TIMESTAMP, v INT) TAGS (a INT = 1, b VARCHAR(8) = 'x');")
        tdSql.query("SELECT tag_name, tag_value FROM information_schema.ins_tags "
                    f"WHERE db_name='{DB}' AND table_name='it_ntb' ORDER BY tag_name;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'a')
        tdSql.checkData(0, 1, '1')
        tdSql.checkData(1, 0, 'b')
        tdSql.checkData(1, 1, 'x')

        # tag-less normal table: no rows (regression: stale decoder state showed phantom tags)
        tdSql.execute("CREATE TABLE it_plain (ts TIMESTAMP, v INT);")
        tdSql.query("SELECT tag_name FROM information_schema.ins_tags "
                    f"WHERE db_name='{DB}' AND table_name='it_plain';")
        tdSql.checkRows(0)

        # SHOW TAGS works on both table kinds, and on a tag-less table (empty)
        tdSql.query("SHOW TAGS FROM it_ntb;")
        tdSql.checkRows(2)
        tdSql.query("SHOW TAGS FROM v_own;")
        tdSql.checkRows(1)
        tdSql.query("SHOW TAGS FROM it_plain;")
        tdSql.checkRows(0)
        tdLog.info("  PASS: ins_tags / SHOW TAGS cover ntb and vntb tags")

    def test_ins_tags_cursor_path_full_scan(self):
        """ins_tags full-DB scan (no table_name equality) walks the meta cursor path:
        vntb owned tags, vntb tag-refs resolved through the pause/resume re-read filler
        (sysTableUserTagsFillNtbRefTagsRow), child-table tags carrying stable_name, and
        tag-less tables producing no rows (phantom-tag decoder regression).

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, tag, ins_tags

        Jira: None

        History:
            - 2026-07-31 Created
        """
        tdSql.execute(f"USE {DB};")
        # tag-less tables must stay invisible on the cursor path even when a tagged
        # table was decoded right before them (decoder-pool state reset regression)
        tdSql.execute("CREATE TABLE cp_plain (ts TIMESTAMP, v INT);")
        tdSql.execute("CREATE VTABLE cp_vnotag (ts TIMESTAMP, val INT FROM src0.val);")

        # no table_name equality -> meta cursor path
        rows = self._rows("SELECT table_name, stable_name, tag_name, tag_value "
                          "FROM information_schema.ins_tags "
                          f"WHERE db_name='{DB}';")
        got = {(t, tag): (stable, val) for t, stable, tag, val in rows}

        expected = {
            # vntb owned tag
            ('v_own', 'loc'): ('None', '5'),
            # vntb tag-refs resolved via the re-read filler
            ('v_ref', 'rcity'): ('None', 'beijing'),
            ('v_ref', 'rcode'): ('None', '100'),
            # mixed owned + ref on one vntb
            ('v_mixed', 'lit'): ('None', '100'),
            ('v_mixed', 'rcity'): ('None', 'beijing'),
            # refs into three different source tables
            ('v_mref', 'c0'): ('None', 'beijing'),
            ('v_mref', 'c1'): ('None', 'shanghai'),
            ('v_mref', 'c2'): ('None', 'guangzhou'),
            # multi-tag vntb
            ('v_ntag', 'name'): ('None', 'alpha'),
            ('v_ntag', 'cnt'): ('None', '7'),
            # child-table control rows: stable_name points at the super table
            ('src0', 'city'): ('src_stb', 'beijing'),
            ('src0', 'code'): ('src_stb', '100'),
            ('src0', 'region'): ('src_stb', 'east'),
            ('src2', 'city'): ('src_stb', 'guangzhou'),
        }
        for key, exp in expected.items():
            assert got.get(key) == exp, f"cursor path: {key} expected {exp}, got {got.get(key)}"
        # tag-less tables produce no rows at all
        leaked = [k for k in got if k[0] in ('cp_plain', 'cp_vnotag')]
        assert not leaked, f"tag-less tables leaked into ins_tags: {leaked}"
        tdLog.info("  PASS: ins_tags cursor path covers owned/ref/mixed/child/tag-less")

    def test_ins_tags_stable_name_semantics(self):
        """stable_name is NULL for normal/virtual-normal tables and filters behave
        accordingly: IS NULL keeps only ntb/vntb rows, equality keeps only child rows.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, tag, ins_tags

        Jira: None

        History:
            - 2026-07-31 Created
        """
        tdSql.execute(f"USE {DB};")

        rows = self._rows("SELECT DISTINCT table_name FROM information_schema.ins_tags "
                          f"WHERE db_name='{DB}' AND stable_name IS NULL;")
        null_names = {r[0] for r in rows}
        assert 'v_own' in null_names, f"vntb missing from stable_name IS NULL: {null_names}"
        assert 'src0' not in null_names, f"child table leaked into IS NULL: {null_names}"

        rows = self._rows("SELECT DISTINCT table_name FROM information_schema.ins_tags "
                          f"WHERE db_name='{DB}' AND stable_name = 'src_stb';")
        stb_names = {r[0] for r in rows}
        assert stb_names == {'src0', 'src1', 'src2'}, f"stable_name equality: {stb_names}"

        rows = self._rows("SELECT DISTINCT table_name FROM information_schema.ins_tags "
                          f"WHERE db_name='{DB}' AND stable_name IS NOT NULL;")
        notnull_names = {r[0] for r in rows}
        assert 'v_own' not in notnull_names, f"vntb leaked into IS NOT NULL: {notnull_names}"
        assert 'src0' in notnull_names, f"child missing from IS NOT NULL: {notnull_names}"
        tdLog.info("  PASS: ins_tags stable_name NULL semantics")

    def test_ins_tags_alter_lifecycle(self):
        """ALTER ADD/SET/DROP TAG is visible in ins_tags (and SHOW TAGS) on the same
        connection, for both physical normal tables and virtual normal tables:
        ADD exposes the tag with a NULL value, SET fills it, DROP removes the row.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, tag, ins_tags, alter

        Jira: None

        History:
            - 2026-07-31 Created
        """
        tdSql.execute(f"USE {DB};")

        def tag_rows(tb):
            # tag_value is VARCHAR in ins_tags: compare as strings, keep NULL as None
            tdSql.query("SELECT tag_name, tag_value FROM information_schema.ins_tags "
                        f"WHERE db_name='{DB}' AND table_name='{tb}' ORDER BY tag_name;")
            return [(tdSql.getData(i, 0),
                     None if tdSql.getData(i, 1) is None else str(tdSql.getData(i, 1)))
                    for i in range(tdSql.queryRows)]

        # physical normal table lifecycle
        tdSql.execute("CREATE TABLE al_ntb (ts TIMESTAMP, v INT) TAGS (a INT = 1);")
        assert tag_rows('al_ntb') == [('a', '1')], f"create: {tag_rows('al_ntb')}"
        tdSql.execute("ALTER TABLE al_ntb ADD TAG b BIGINT;")
        assert tag_rows('al_ntb') == [('a', '1'), ('b', None)], f"add: {tag_rows('al_ntb')}"
        tdSql.execute("ALTER TABLE al_ntb SET TAG b = 42;")
        assert tag_rows('al_ntb') == [('a', '1'), ('b', '42')], f"set: {tag_rows('al_ntb')}"
        tdSql.execute("ALTER TABLE al_ntb DROP TAG a;")
        assert tag_rows('al_ntb') == [('b', '42')], f"drop: {tag_rows('al_ntb')}"

        # virtual normal table lifecycle (owned tags)
        tdSql.execute("CREATE VTABLE al_vtb (ts TIMESTAMP, val INT FROM src0.val) TAGS (x INT = 1);")
        assert tag_rows('al_vtb') == [('x', '1')], f"v create: {tag_rows('al_vtb')}"
        tdSql.execute("ALTER TABLE al_vtb ADD TAG y VARCHAR(8);")
        assert tag_rows('al_vtb') == [('x', '1'), ('y', None)], f"v add: {tag_rows('al_vtb')}"
        tdSql.execute("ALTER TABLE al_vtb SET TAG y = 'hi';")
        assert tag_rows('al_vtb') == [('x', '1'), ('y', 'hi')], f"v set: {tag_rows('al_vtb')}"
        tdSql.execute("ALTER TABLE al_vtb DROP TAG x;")
        assert tag_rows('al_vtb') == [('y', 'hi')], f"v drop: {tag_rows('al_vtb')}"

        # SHOW TAGS mirrors the final state
        tdSql.query("SHOW TAGS FROM al_ntb;")
        tdSql.checkRows(1)
        tdSql.query("SHOW TAGS FROM al_vtb;")
        tdSql.checkRows(1)
        tdLog.info("  PASS: ins_tags reflects ALTER ADD/SET/DROP TAG same-connection")

    def test_ins_tags_type_rendering(self):
        """ins_tags renders tag_type (with length suffix for var types) and tag_value
        for the common owned-tag types on a normal table.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, tag, ins_tags, datatype

        Jira: None

        History:
            - 2026-07-31 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE ty_ntb (ts TIMESTAMP, v INT) TAGS ("
                      "t_big BIGINT = 9000000000, t_bool BOOL = true, "
                      "t_double DOUBLE = 2.25, t_float FLOAT = 1.5, "
                      "t_int INT = 7, t_nch NCHAR(10) = '汉字', "
                      "t_small SMALLINT = -300, t_tiny TINYINT = -8, "
                      "t_ts TIMESTAMP = 1700000000000, t_vch VARCHAR(8) = 'abc');")

        tdSql.query("SELECT tag_name, tag_type, tag_value FROM information_schema.ins_tags "
                    f"WHERE db_name='{DB}' AND table_name='ty_ntb' ORDER BY tag_name;")
        actual = [(tdSql.getData(i, 0), tdSql.getData(i, 1), str(tdSql.getData(i, 2)))
                  for i in range(tdSql.queryRows)]
        expected = [
            ('t_big', 'BIGINT', '9000000000'),
            ('t_bool', 'BOOL', 'true'),
            ('t_double', 'DOUBLE', '2.250000000'),
            ('t_float', 'FLOAT', '1.50000'),
            ('t_int', 'INT', '7'),
            ('t_nch', 'NCHAR(10)', '汉字'),
            ('t_small', 'SMALLINT', '-300'),
            ('t_tiny', 'TINYINT', '-8'),
            ('t_ts', 'TIMESTAMP', '1700000000000'),
            ('t_vch', 'VARCHAR(8)', 'abc'),
        ]
        assert actual == expected, f"type rendering mismatch\n  expected {expected}\n  actual   {actual}"
        tdLog.info("  PASS: ins_tags tag_type/tag_value rendering")

    def test_ins_tags_ref_set_converts_to_owned(self):
        """SET TAG on a vntb tag-ref converts it to an owned tag: ins_tags then serves
        the literal from the owned-value branch (hasRef cleared) on both the fast path
        (table_name equality) and the cursor path (full scan).

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, tag, ins_tags, tag_ref

        Jira: None

        History:
            - 2026-07-31 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE VTABLE rc_vtb (ts TIMESTAMP, val INT FROM src0.val) "
                      "TAGS (rcity NCHAR(20) FROM src0.city);")

        # ref resolves to the source value
        tdSql.query("SELECT tag_value FROM information_schema.ins_tags "
                    f"WHERE db_name='{DB}' AND table_name='rc_vtb';")
        tdSql.checkData(0, 0, 'beijing')

        # SET converts ref -> owned literal
        tdSql.execute("ALTER TABLE rc_vtb SET TAG rcity = 'shenzhen';")
        tdSql.query("SELECT tag_value FROM information_schema.ins_tags "
                    f"WHERE db_name='{DB}' AND table_name='rc_vtb';")
        tdSql.checkData(0, 0, 'shenzhen')

        # cursor path sees the same converted value
        rows = self._rows("SELECT tag_value FROM information_schema.ins_tags "
                          f"WHERE db_name='{DB}' AND stable_name IS NULL "
                          "AND table_name='rc_vtb';")
        assert rows == [('shenzhen',)], f"cursor path after ref->owned: {rows}"
        tdLog.info("  PASS: ins_tags serves converted ref->owned tag on both paths")

    def test_ins_tags_multi_db_isolation(self):
        """ins_tags rows are isolated by db_name: same-named tables with different tag
        values in two databases do not leak into each other's result set.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, tag, ins_tags

        Jira: None

        History:
            - 2026-07-31 Created
        """
        DB2 = "td_vntb_tags_iso"
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB2};")
        tdSql.execute(f"CREATE DATABASE {DB2} BUFFER 16;")
        tdSql.execute(f"CREATE TABLE {DB2}.iso_ntb (ts TIMESTAMP, v INT) TAGS (k INT = 9);")
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE iso_ntb (ts TIMESTAMP, v INT) TAGS (k INT = 1);")

        # unfiltered by db: both databases' rows show up under the same table name
        rows = self._rows("SELECT db_name, tag_value FROM information_schema.ins_tags "
                          "WHERE table_name='iso_ntb';")
        assert sorted(rows) == sorted([(DB, '1'), (DB2, '9')]), f"cross-db rows: {rows}"

        # db_name filter isolates each side
        tdSql.query("SELECT tag_value FROM information_schema.ins_tags "
                    f"WHERE db_name='{DB}' AND table_name='iso_ntb';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '1')
        tdSql.query("SELECT tag_value FROM information_schema.ins_tags "
                    f"WHERE db_name='{DB2}' AND table_name='iso_ntb';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '9')

        tdSql.execute(f"DROP DATABASE {DB2};")
        tdLog.info("  PASS: ins_tags db_name isolation")

    # ==================================================================
    # ERROR MATRIX — parser / meta guards not previously exercised
    # ==================================================================

    def test_set_on_tag_ref_converts_to_owned(self):
        """SET TAG on a tag-ref converts it to an owned tag (ref -> literal): the reference
        is cleared and the static value takes over, mirroring the virtual-child-table behavior.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, alter, tag_ref, convert

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE VTABLE vctb_setref (ts TIMESTAMP, val INT FROM src0.val) "
                      "TAGS (rcity NCHAR(20) FROM src0.city);")
        self._check_values("SELECT DISTINCT rcity FROM vctb_setref;", [('beijing',)],
                           "initial ref value")
        # SET on a tag-ref: converts to owned (ref cleared), value visible on the same conn
        tdSql.execute("ALTER TABLE vctb_setref SET TAG rcity = 'x';")
        self._check_values("SELECT DISTINCT rcity FROM vctb_setref;", [('x',)],
                           "ref converted to literal")
        # positive control: an owned tag on the same table is settable too
        tdSql.execute("ALTER TABLE vctb_setref ADD TAG own INT;")
        tdSql.execute("ALTER TABLE vctb_setref SET TAG own = 9;")
        self._check_values("SELECT DISTINCT own FROM vctb_setref;", [(9,)], "owned tag settable (positive control)")
        tdLog.info("  PASS: SET TAG on tag-ref converts to owned")

    def test_error_ext_json_option_rules(self):
        """Error: external-source tag-ref, ALTER-time JSON tags, and column options on tags
        are explicitly rejected (review-driven rules).

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, tag, alter, negative

        Jira: None

        History:
            - 2026-07-28 Created
        """
        tdSql.execute(f"USE {DB};")
        # external source (4-segment) tag-ref is rejected at CREATE and at ALTER
        tdSql.error("CREATE VTABLE vctb_ext1 (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (r NCHAR(20) FROM s.db.src0.city);")
        tdSql.execute("CREATE VTABLE vctb_ext2 (ts TIMESTAMP, val INT FROM src0.val) TAGS (a INT = 0);")
        tdSql.error("ALTER TABLE vctb_ext2 ADD TAG r NCHAR(20) FROM s.db.src0.city;")
        tdSql.error("ALTER TABLE vctb_ext2 SET TAG a = s.db.src0.code;")
        # column options other than FROM are rejected on tags
        tdSql.error("CREATE VTABLE vctb_opt (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (t INT PRIMARY KEY);")
        tdSql.error("CREATE VTABLE vctb_opt2 (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (t INT COMMENT 'x');")
        # JSON tag is allowed only as the single tag at CREATE; ALTER cannot add a JSON tag,
        # and no tag can be added to a table that already has a JSON tag
        tdSql.execute("CREATE VTABLE vctb_json (ts TIMESTAMP, val INT FROM src0.val) TAGS (j JSON = '{\"k\":1}');")
        tdSql.error("ALTER TABLE vctb_json ADD TAG k INT;")
        tdSql.execute("CREATE VTABLE vctb_json2 (ts TIMESTAMP, val INT FROM src0.val) TAGS (a INT = 0);")
        tdSql.error("ALTER TABLE vctb_json2 ADD TAG j JSON;")
        tdLog.info("  PASS: ext/json/option tag rules enforced")

    def test_error_tag_bytes_exceed_max_tags_len(self):
        """Error: total tag bytes exceeding TSDB_MAX_TAGS_LEN (16384) is rejected at CREATE
        (checkCreateTags). Two VARCHAR(8192) tags sum past the cap while each stays well under
        the per-column cap, so this specifically exercises the total-bytes guard.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, create, tag, boundary, negative

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.error("CREATE VTABLE vctb_em_len (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (a VARCHAR(8192) = 'x', b VARCHAR(8192) = 'y');")

    def test_error_varchar_tag_over_length(self):
        """Error: a VARCHAR tag longer than TSDB_MAX_BINARY_LEN is rejected at CREATE.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, create, tag, boundary, negative

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.error("CREATE VTABLE vctb_em_vl (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (t VARCHAR(66000) = 'x');")

    def test_error_nchar_tag_over_length(self):
        """Error: an NCHAR tag longer than TSDB_MAX_NCHAR_LEN is rejected at CREATE
        (NCHAR byte length = N * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE).

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, create, tag, boundary, negative

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.error("CREATE VTABLE vctb_em_nl (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (t NCHAR(17000) = 'x');")

    def test_error_tag_ref_type_mismatch(self):
        """Error: a tag-ref whose declared type differs from the source tag type is rejected
        (checkTagRef). src0.city is NCHAR(20); declaring the ref as INT is a type mismatch.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, create, tag_ref, negative

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.error("CREATE VTABLE vctb_em_tm (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (r INT FROM src0.city);")

    # ==================================================================
    # RESTART PERSISTENCE — the bit-5 trailer + monotonic schemaTag.version
    # must re-decode correctly after a taosd restart.
    # ==================================================================

    def _restart_dnode(self):
        try:
            tdDnodes.stop(1)
            tdDnodes.start(1)
        except Exception as e:
            # A failed restart must fail the test, not silently skip the persistence
            # assertions (the bit-5 trailer + version re-decode this suite exists to pin).
            raise AssertionError(f"[tags-persistence] dnode restart failed: {e}")
        # readiness poll instead of a fixed sleep (flaky on loaded CI runners)
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                tdSql.query("SELECT SERVER_STATUS();")
                if tdSql.queryResult and tdSql.queryResult[0][0] == 1:
                    return
            except Exception:
                pass
            time.sleep(0.5)
        raise AssertionError("[tags-persistence] dnode not ready 30s after restart")

    def test_restart_owned_tag_and_ref_persist(self):
        """Restart: owned tag values + tag-ref resolution survive a taosd restart — the on-disk
        bit-5 trailer and monotonic schemaTag.version re-decode correctly.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, persistence, restart, tag, tag_ref

        Jira: None

        History:
            - 2026-07-27 Created
        """
        self._restart_dnode()
        tdSql.execute(f"USE {DB};")
        # owned tag value (v_own.loc = 5) survives — DISTINCT collapses the per-row constant to 1 row
        self._check_values("SELECT DISTINCT loc FROM v_own;", [(5,)], "restart: owned tag value")
        # tag-ref still resolves to the source (v_ref.rcity -> src0.city = 'beijing')
        self._check_values("SELECT DISTINCT rcity FROM v_ref;", [('beijing',)], "restart: tag-ref follows source")
        # schema re-decoded from the trailer: DESC still shows the tags
        rows = self._rows("DESC v_own;")
        names = [r[0] for r in rows]
        assert 'loc' in names, f"restart: owned tag 'loc' missing from DESC: {rows}"
        tdLog.info("  PASS: restart preserves owned tags + tag-refs")

    def test_restart_drop_all_tags_then_readd(self):
        """Restart: after dropping every tag (schemaTag.nCols=0, version stays monotonic, empty
        STag trailer), the table stays readable across restart and a tag can be re-added — the
        case where the empty-schema trailer must re-decode and the version must stay monotonic.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, persistence, restart, tag, alter

        Jira: None

        History:
            - 2026-07-27 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE ntb_rst (ts TIMESTAMP, v INT);")
        tdSql.execute(f"INSERT INTO ntb_rst VALUES ({TS0}, 1);")
        tdSql.execute("ALTER TABLE ntb_rst ADD TAG t1 INT;")
        tdSql.execute("ALTER TABLE ntb_rst SET TAG t1 = 42;")
        # drop the only tag -> empty schemaTag, monotonic version, empty STag trailer
        tdSql.execute("ALTER TABLE ntb_rst DROP TAG t1;")
        self._check_count("SELECT count(*) FROM ntb_rst;", 1, "pre-restart: readable after drop-all-tags")
        self._restart_dnode()
        tdSql.execute(f"USE {DB};")
        # table still readable after restart (empty-schema trailer re-decoded)
        self._check_count("SELECT count(*) FROM ntb_rst;", 1, "restart: readable after drop-all-tags")
        rows = self._rows("DESC ntb_rst;")
        n_tags = len([r for r in rows if r[3] == 'TAG'])
        assert n_tags == 0, f"restart: expected 0 tags after drop-all, DESC shows {n_tags}: {rows}"
        # re-add a tag after restart (version continues monotonically; client catalog refreshed)
        tdSql.execute("ALTER TABLE ntb_rst ADD TAG t2 VARCHAR(16);")
        tdSql.execute("ALTER TABLE ntb_rst SET TAG t2 = 'post';")
        self._check_values("SELECT t2, v FROM ntb_rst;", [('post', 1)], "restart: re-add tag after drop-all")
        tdLog.info("  PASS: restart after drop-all-tags + re-add")

    def test_auth_tag_ref_source_select(self):
        """Privilege gate: a user without SELECT on the source db is denied CREATE VTABLE
        tag/col refs and ALTER ADD TAG ... FROM; granting READ on the source db lets both
        through (covers authColDefRefs + the authAlterTable ref-alter branch).

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, tag, tag_ref, auth, permission

        Jira: None

        History:
            - 2026-08-14 Created
        """
        DB_PRIV = "td_vntb_tags_priv"
        USER = "u_vntag"
        try:
            tdSql.execute(f"DROP DATABASE IF EXISTS {DB_PRIV};")
            tdSql.execute(f"DROP USER IF EXISTS {USER};")
            # target-db privileges only: no READ on {DB} where src0/src1 live
            tdSql.execute(f"CREATE USER {USER} PASS 'Taosdata_123';")
            tdSql.execute(f"CREATE DATABASE {DB_PRIV};")
            try:
                tdSql.execute(f"GRANT READ, WRITE, CREATE TABLE, ALTER TABLE ON {DB_PRIV} TO {USER};")
            except Exception as e:
                # GRANT is enterprise-only; without it a non-root user cannot be given the
                # target-db privileges this test needs, so the deny/grant paths are untestable
                # on community builds — skip visibly rather than pass vacuously.
                pytest.skip(f"GRANT not supported on this build, cannot test tag-ref privilege gate: {e}")

            tdSql.connect(user=USER, password='Taosdata_123')
            tdSql.execute(f"USE {DB_PRIV};")
            # deny: CREATE VTABLE whose col/tag refs read {DB}.srcN without SELECT on {DB}
            tdSql.error("CREATE VTABLE vctb_auth (ts TIMESTAMP, val INT FROM td_vntb_tags.src0.val) "
                        "TAGS (r INT FROM td_vntb_tags.src0.code);")

            # allow: after READ on the source db, the same statement succeeds
            tdSql.connect(user="root", password="taosdata")
            tdSql.execute(f"GRANT READ ON {DB} TO {USER};")
            tdSql.connect(user=USER, password='Taosdata_123')
            tdSql.execute(f"USE {DB_PRIV};")
            tdSql.execute("CREATE VTABLE vctb_auth (ts TIMESTAMP, val INT FROM td_vntb_tags.src0.val) "
                          "TAGS (r INT FROM td_vntb_tags.src0.code);")

            # deny again: ALTER TABLE ADD TAG ... FROM after READ is revoked
            tdSql.connect(user="root", password="taosdata")
            tdSql.execute(f"REVOKE READ ON {DB} FROM {USER};")
            tdSql.connect(user=USER, password='Taosdata_123')
            tdSql.execute(f"USE {DB_PRIV};")
            tdSql.error(f"ALTER TABLE vctb_auth ADD TAG r2 INT FROM {DB}.src1.code;")

            # re-grant and clean up as the user, then drop as root
            tdSql.connect(user="root", password="taosdata")
            tdSql.execute(f"GRANT READ ON {DB} TO {USER};")
            tdSql.connect(user=USER, password='Taosdata_123')
            tdSql.execute(f"USE {DB_PRIV};")
            tdSql.execute(f"ALTER TABLE vctb_auth ADD TAG r2 INT FROM {DB}.src1.code;")
            tdSql.execute("DROP TABLE vctb_auth;")
            tdLog.info("  PASS: tag-ref source SELECT privilege gate (deny -> grant -> deny -> grant)")
        finally:
            tdSql.connect(user="root", password="taosdata")
            tdSql.execute(f"DROP DATABASE IF EXISTS {DB_PRIV};")
            tdSql.execute(f"DROP USER IF EXISTS {USER};")

    @classmethod
    def teardown_class(cls):
        tdLog.info("=== teardown: dropping database ===")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB};")
