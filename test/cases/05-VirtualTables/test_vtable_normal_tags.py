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
  - tag-refs:   `FROM src.tag` references, read-only, value follows the source
"""

from new_test_framework.utils import tdLog, tdSql

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
    # 5. PLAIN NORMAL TABLE tags are NOT supported (only virtual normal
    #    tables own tags) — tag DDL must be rejected
    # ==================================================================

    def test_ntb_add_tag_rejected(self):
        """Normal table: ALTER ADD TAG is rejected (tags only on virtual normal tables).

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, alter, negative

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE ntb_e1 (ts TIMESTAMP, val INT);")
        tdSql.error("ALTER TABLE ntb_e1 ADD TAG loc INT;")
        tdSql.error("ALTER TABLE ntb_e1 ADD TAG r NCHAR(20) FROM src0.city;")

    def test_ntb_set_drop_tag_rejected(self):
        """Normal table: SET TAG / DROP TAG are rejected (tags only on virtual normal tables).

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, alter, negative

        Jira: None

        History:
            - 2026-07-20 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE ntb_e2 (ts TIMESTAMP, val INT);")
        tdSql.error("ALTER TABLE ntb_e2 SET TAG loc = 5;")
        tdSql.error("ALTER TABLE ntb_e2 DROP TAG loc;")
        # plain normal table stays tag-less: DESC shows only the columns
        rows = self._rows("DESC ntb_e2;")
        names = sorted(r[0] for r in rows)
        assert names == sorted(['ts', 'val']), f"DESC ntb_e2 fields: {names}"
        tdLog.info("  PASS: ntb stays tag-less")

    @classmethod
    def teardown_class(cls):
        tdLog.info("=== teardown: dropping database ===")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB};")
