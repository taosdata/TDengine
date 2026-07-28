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

    def _distinct(self, sql):
        return sorted(self._rows(sql))

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
                      "TAGS (own INT);")
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
                    "TAGS (d DECIMAL(10,2));")

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
                    "TAGS (a INT, a BIGINT);")

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
        tdSql.error("CREATE VTABLE vctb_e2 (ts TIMESTAMP, dup INT) TAGS (dup INT);")

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
        """Normal table: DESC marks owned tags; SHOW CREATE emits replayable ALTER stmts.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, desc, show_create

        Jira: None

        History:
            - 2026-07-27 Created
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

        # SHOW CREATE for a normal table emits the CREATE plus replayable ALTER statements
        # (an inline TAGS clause on CREATE TABLE would create a super table instead).
        tdSql.query("SHOW CREATE TABLE ntb_p3;")
        create_sql = str(tdSql.getData(0, 1))
        assert create_sql.startswith("CREATE TABLE `ntb_p3`"), f"SHOW CREATE ntb_p3: {create_sql}"
        assert "ALTER TABLE `ntb_p3` ADD TAG `loc` INT" in create_sql, \
            f"SHOW CREATE ntb_p3 ADD TAG loc: {create_sql}"
        assert "ALTER TABLE `ntb_p3` ADD TAG `owner` VARCHAR(16)" in create_sql, \
            f"SHOW CREATE ntb_p3 ADD TAG owner: {create_sql}"
        assert 'ALTER TABLE `ntb_p3` SET TAG `owner` = "alice"' in create_sql, \
            f"SHOW CREATE ntb_p3 SET TAG owner: {create_sql}"
        tdLog.info(f"  PASS: SHOW CREATE emits ALTER stmts: {create_sql}")

        # round-trip: drop the table, replay every emitted statement, expect identical tags
        tdSql.execute("DROP TABLE ntb_p3;")
        for stmt in create_sql.split("; "):
            if stmt.strip():
                tdSql.execute(stmt)
        rows = self._rows("DESC ntb_p3;")
        tag_names = sorted(r[0] for r in rows if r[3] == 'TAG')
        assert tag_names == ['loc', 'owner'], f"round-trip DESC ntb_p3 tag rows: {tag_names}"
        # the recreated table is empty; insert a row to read back the replayed tag value
        tdSql.execute(f"INSERT INTO ntb_p3 VALUES ({TS0}, 1);")
        self._check_values("SELECT loc, owner, val FROM ntb_p3;", [(None, 'alice', 1)],
                           "round-trip owned tag value")
        tdLog.info("  PASS: SHOW CREATE round-trip restores tags")

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
        tdSql.execute("CREATE VTABLE vc3 (ts TIMESTAMP, val INT FROM src0.val) TAGS (lit INT);")
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
        tdSql.execute("CREATE VTABLE vc4 (ts TIMESTAMP, val INT FROM src0.val) TAGS (t INT);")
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
        tdSql.execute("CREATE VTABLE vctb_ext2 (ts TIMESTAMP, val INT FROM src0.val) TAGS (a INT);")
        tdSql.error("ALTER TABLE vctb_ext2 ADD TAG r NCHAR(20) FROM s.db.src0.city;")
        tdSql.error("ALTER TABLE vctb_ext2 SET TAG a = s.db.src0.code;")
        # column options other than FROM are rejected on tags
        tdSql.error("CREATE VTABLE vctb_opt (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (t INT PRIMARY KEY);")
        tdSql.error("CREATE VTABLE vctb_opt2 (ts TIMESTAMP, val INT FROM src0.val) "
                    "TAGS (t INT COMMENT 'x');")
        # JSON tag is allowed only as the single tag at CREATE; ALTER cannot add a JSON tag,
        # and no tag can be added to a table that already has a JSON tag
        tdSql.execute("CREATE VTABLE vctb_json (ts TIMESTAMP, val INT FROM src0.val) TAGS (j JSON);")
        tdSql.error("ALTER TABLE vctb_json ADD TAG k INT;")
        tdSql.execute("CREATE VTABLE vctb_json2 (ts TIMESTAMP, val INT FROM src0.val) TAGS (a INT);")
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
                    "TAGS (a VARCHAR(8192), b VARCHAR(8192));")

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
                    "TAGS (t VARCHAR(66000));")

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
                    "TAGS (t NCHAR(17000));")

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
            time.sleep(3)
        except Exception as e:
            tdLog.info(f"[tags-persistence] dnode restart skipped: {e}")
            return False
        return True

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
        if not self._restart_dnode():
            return
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
        if not self._restart_dnode():
            return
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

    @classmethod
    def teardown_class(cls):
        tdLog.info("=== teardown: dropping database ===")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB};")
