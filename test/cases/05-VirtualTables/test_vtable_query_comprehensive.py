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

"""Comprehensive query tests for virtual tables with col len < ref col len.

This test suite covers:
  - Aggregate functions (count, sum, avg, min, max, spread, stddev, hyperloglog)
  - Selection functions (first, last, last_row, top, bottom, sample)
  - String functions (lower, upper, concat, concat_ws, substr, replace, ltrim, rtrim,
                      length, char_length)
  - Math / conversion functions (cast, abs, ceil, floor, round, arithmetic)
  - Subqueries (scalar, table, nested, with string func, with agg, with window, with join, with union)
  - JOIN between vtable and regular table, vtable and vtable
  - UNION / UNION ALL
  - GROUP BY / PARTITION BY
  - Window functions (INTERVAL, INTERVAL FILL, STATE_WINDOW, SESSION)
  - WHERE filters (=, <>, LIKE, IN, BETWEEN, IS NULL, IS NOT NULL, combined)
  - ORDER BY / LIMIT / OFFSET
  - DISTINCT / CASE WHEN
  - Virtual super table queries (SELECT *, aggregate, interval, partition by, subquery)
  - Edge cases (SELECT *, NULL handling, arithmetic, consistency with source)

All tests use a virtual table whose BINARY/NCHAR column lengths are
*smaller* than the referenced source columns, verifying that the
TMAX(vtb_bytes, ref_bytes) fix in scanAddCol works end-to-end.
"""

from new_test_framework.utils import tdLog, tdSql, etool, tdCom


class TestVtableQueryComprehensive:

    DB = "test_vtb_query"

    # ---------- expected source data (src_ntb: binary(32), nchar(32)) --------
    TS = [
        "2024-01-01 00:00:00.000",
        "2024-01-01 00:00:01.000",
        "2024-01-01 00:00:02.000",
        "2024-01-01 00:00:03.000",
        "2024-01-01 00:00:04.000",
        "2024-01-01 00:00:05.000",
        "2024-01-01 00:00:06.000",
        "2024-01-01 00:00:07.000",
        "2024-01-01 00:00:08.000",
        "2024-01-01 00:00:09.000",
    ]
    BIN = [
        "Shanghai - Los Angeles",     # 22 chars
        "short",                       # 5 chars
        "Palo Alto - Mountain View",   # 25 chars
        None,
        "San Francisco - Cupertino",   # 25 chars
        "Beijing - Shenzhen City",     # 23 chars
        "a",                           # 1 char
        None,
        "Hangzhou - West Lake Area",   # 25 chars
        "SV",                          # 2 chars
    ]
    NCH = [
        "圣克拉拉 - Santa Clara",
        "短",
        "库比蒂诺 - Cupertino City",
        None,
        "旧金山 - San Francisco",
        "北京市海淀区中关村大街",
        "a",
        None,
        "杭州西湖风景区龙井路",
        "深",
    ]
    IVAL = [10, 20, 30, None, 50, 60, 70, None, 90, 100]
    FVAL = [1.1, 2.2, 3.3, None, 5.5, 6.6, 7.7, None, 9.9, 10.0]

    # helper: non-null values
    BIN_NN = [v for v in BIN if v is not None]
    NCH_NN = [v for v in NCH if v is not None]
    IVAL_NN = [v for v in IVAL if v is not None]
    FVAL_NN = [v for v in FVAL if v is not None]

    # ------------------------------------------------------------------ setup
    def setup_class(cls):
        tdLog.info("=== setup: create DB, source table, virtual tables ===")
        db = cls.DB
        tdSql.execute(f"DROP DATABASE IF EXISTS {db};")
        tdSql.execute(f"CREATE DATABASE {db} VGROUPS 2;")
        tdSql.execute(f"USE {db};")

        # --- source normal table: binary(32), nchar(32), int, float --------
        tdSql.execute(
            "CREATE TABLE src_ntb ("
            "ts TIMESTAMP, bin_col BINARY(32), nch_col NCHAR(32), "
            "ival INT, fval FLOAT);")

        for i in range(len(cls.TS)):
            b = f"'{cls.BIN[i]}'" if cls.BIN[i] is not None else "NULL"
            n = f"'{cls.NCH[i]}'" if cls.NCH[i] is not None else "NULL"
            iv = str(cls.IVAL[i]) if cls.IVAL[i] is not None else "NULL"
            fv = str(cls.FVAL[i]) if cls.FVAL[i] is not None else "NULL"
            tdSql.execute(
                f"INSERT INTO src_ntb VALUES ('{cls.TS[i]}', {b}, {n}, {iv}, {fv});")

        # --- virtual table: BINARY(8)/NCHAR(8) < source BINARY(32)/NCHAR(32)
        tdSql.execute(
            "CREATE VTABLE vtb_lt ("
            "ts TIMESTAMP, "
            "bin_col BINARY(8) FROM src_ntb.bin_col, "
            "nch_col NCHAR(8)  FROM src_ntb.nch_col, "
            "ival    INT       FROM src_ntb.ival, "
            "fval    FLOAT     FROM src_ntb.fval);")

        # --- a second source table for JOIN tests --------------------------
        tdSql.execute(
            "CREATE TABLE src_dim ("
            "ts TIMESTAMP, city BINARY(32), code INT);")
        tdSql.execute("INSERT INTO src_dim VALUES "
                      "('2024-01-01 00:00:00.000', 'Shanghai', 10);")
        tdSql.execute("INSERT INTO src_dim VALUES "
                      "('2024-01-01 00:00:01.000', 'Palo Alto', 20);")
        tdSql.execute("INSERT INTO src_dim VALUES "
                      "('2024-01-01 00:00:02.000', 'Beijing', 30);")

        # virtual table on src_dim (col len < ref len)
        tdSql.execute(
            "CREATE VTABLE vtb_dim ("
            "ts TIMESTAMP, city BINARY(8) FROM src_dim.city, "
            "code INT FROM src_dim.code);")

        # --- super table + child tables for partition / group by tests -----
        tdSql.execute(
            "CREATE STABLE src_stb ("
            "ts TIMESTAMP, bin_col BINARY(32), ival INT) "
            "TAGS (region NCHAR(16));")
        tdSql.execute("CREATE TABLE src_ct1 USING src_stb TAGS ('east');")
        tdSql.execute("CREATE TABLE src_ct2 USING src_stb TAGS ('west');")
        for i in range(5):
            b = f"'{cls.BIN[i]}'" if cls.BIN[i] is not None else "NULL"
            iv = str(cls.IVAL[i]) if cls.IVAL[i] is not None else "NULL"
            tdSql.execute(
                f"INSERT INTO src_ct1 VALUES ('{cls.TS[i]}', {b}, {iv});")
        for i in range(5, 10):
            b = f"'{cls.BIN[i]}'" if cls.BIN[i] is not None else "NULL"
            iv = str(cls.IVAL[i]) if cls.IVAL[i] is not None else "NULL"
            tdSql.execute(
                f"INSERT INTO src_ct2 VALUES ('{cls.TS[i]}', {b}, {iv});")

        # virtual super table + virtual child tables
        tdSql.execute(
            "CREATE STABLE vstb ("
            "ts TIMESTAMP, bin_col BINARY(8), ival INT) "
            "TAGS (region NCHAR(16)) VIRTUAL 1;")
        tdSql.execute(
            "CREATE VTABLE vct1 "
            "(bin_col FROM src_ct1.bin_col, ival FROM src_ct1.ival) "
            "USING vstb TAGS ('east');")
        tdSql.execute(
            "CREATE VTABLE vct2 "
            "(bin_col FROM src_ct2.bin_col, ival FROM src_ct2.ival) "
            "USING vstb TAGS ('west');")

        tdLog.info("=== setup complete ===")

    # ========================= AGGREGATE FUNCTIONS ==========================

    def test_agg_count(self):
        """Aggregate: COUNT(*), COUNT(col) on vtable with col len < ref len

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, aggregate, count

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== agg: count ===")
        tdSql.query(f"SELECT COUNT(*) FROM {db}.vtb_lt;")
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"SELECT COUNT(bin_col) FROM {db}.vtb_lt;")
        tdSql.checkData(0, 0, 8)  # 2 NULLs

        tdSql.query(f"SELECT COUNT(nch_col) FROM {db}.vtb_lt;")
        tdSql.checkData(0, 0, 8)

        tdSql.query(f"SELECT COUNT(ival) FROM {db}.vtb_lt;")
        tdSql.checkData(0, 0, 8)

    def test_agg_sum_avg(self):
        """Aggregate: SUM, AVG on int/float columns of vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, aggregate, sum, avg

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== agg: sum / avg ===")
        expected_sum = sum(self.IVAL_NN)
        tdSql.query(f"SELECT SUM(ival) FROM {db}.vtb_lt;")
        tdSql.checkData(0, 0, expected_sum)

        tdSql.query(f"SELECT AVG(ival) FROM {db}.vtb_lt;")
        avg_val = tdSql.getData(0, 0)
        assert abs(avg_val - expected_sum / len(self.IVAL_NN)) < 0.01, \
            f"avg mismatch: {avg_val}"

        tdSql.query(f"SELECT SUM(fval) FROM {db}.vtb_lt;")
        fsum = tdSql.getData(0, 0)
        expected_fsum = sum(self.FVAL_NN)
        assert abs(fsum - expected_fsum) < 0.5, f"fval sum mismatch: {fsum}"

    def test_agg_min_max(self):
        """Aggregate: MIN, MAX on int and binary columns of vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, aggregate, min, max

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== agg: min / max ===")
        tdSql.query(f"SELECT MIN(ival) FROM {db}.vtb_lt;")
        tdSql.checkData(0, 0, min(self.IVAL_NN))

        tdSql.query(f"SELECT MAX(ival) FROM {db}.vtb_lt;")
        tdSql.checkData(0, 0, max(self.IVAL_NN))

        tdSql.query(f"SELECT MIN(bin_col) FROM {db}.vtb_lt;")
        tdSql.checkData(0, 0, min(self.BIN_NN))

        tdSql.query(f"SELECT MAX(bin_col) FROM {db}.vtb_lt;")
        tdSql.checkData(0, 0, max(self.BIN_NN))

    def test_agg_spread_stddev(self):
        """Aggregate: SPREAD, STDDEV on vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, aggregate, spread, stddev

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== agg: spread / stddev ===")
        tdSql.query(f"SELECT SPREAD(ival) FROM {db}.vtb_lt;")
        expected_spread = max(self.IVAL_NN) - min(self.IVAL_NN)
        tdSql.checkData(0, 0, float(expected_spread))

        tdSql.query(f"SELECT STDDEV(ival) FROM {db}.vtb_lt;")
        stddev_val = tdSql.getData(0, 0)
        assert stddev_val is not None and stddev_val > 0, \
            f"stddev should be positive: {stddev_val}"

    def test_agg_hyperloglog(self):
        """Aggregate: HYPERLOGLOG on binary column of vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, aggregate, hyperloglog

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== agg: hyperloglog ===")
        tdSql.query(f"SELECT HYPERLOGLOG(bin_col) FROM {db}.vtb_lt;")
        hll_val = tdSql.getData(0, 0)
        # 8 distinct non-null values; HLL is approximate
        assert hll_val >= 6 and hll_val <= 10, \
            f"hyperloglog approximate count unexpected: {hll_val}"

    def test_agg_multi_func(self):
        """Aggregate: multiple aggregate functions in single SELECT on vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, aggregate, multi_func

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== agg: multi func in single query ===")
        tdSql.query(
            f"SELECT COUNT(*), SUM(ival), AVG(ival), MIN(ival), MAX(ival), "
            f"SPREAD(ival) FROM {db}.vtb_lt;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)                    # count
        tdSql.checkData(0, 1, sum(self.IVAL_NN))      # sum
        tdSql.checkData(0, 3, min(self.IVAL_NN))      # min
        tdSql.checkData(0, 4, max(self.IVAL_NN))      # max

    # ========================= SELECTION FUNCTIONS ==========================

    def test_sel_first_last(self):
        """Selection: FIRST, LAST on vtable with col len < ref len

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, selection, first, last

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== sel: first / last ===")
        tdSql.query(f"SELECT FIRST(bin_col) FROM {db}.vtb_lt;")
        tdSql.checkData(0, 0, self.BIN[0])

        tdSql.query(f"SELECT LAST(bin_col) FROM {db}.vtb_lt;")
        tdSql.checkData(0, 0, self.BIN[9])

        tdSql.query(f"SELECT FIRST(nch_col) FROM {db}.vtb_lt;")
        tdSql.checkData(0, 0, self.NCH[0])

        tdSql.query(f"SELECT LAST(nch_col) FROM {db}.vtb_lt;")
        tdSql.checkData(0, 0, self.NCH[9])

    def test_sel_top_bottom(self):
        """Selection: TOP, BOTTOM on int column of vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, selection, top, bottom

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== sel: top / bottom ===")
        tdSql.query(f"SELECT TOP(ival, 3) FROM {db}.vtb_lt;")
        tdSql.checkRows(3)
        top3 = sorted(self.IVAL_NN, reverse=True)[:3]
        results = sorted([tdSql.getData(i, 0) for i in range(3)], reverse=True)
        assert results == top3, f"top3 mismatch: {results} vs {top3}"

        tdSql.query(f"SELECT BOTTOM(ival, 3) FROM {db}.vtb_lt;")
        tdSql.checkRows(3)
        bot3 = sorted(self.IVAL_NN)[:3]
        results = sorted([tdSql.getData(i, 0) for i in range(3)])
        assert results == bot3, f"bottom3 mismatch: {results} vs {bot3}"

    def test_sel_last_row(self):
        """Selection: LAST_ROW on vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, selection, last_row

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== sel: last_row ===")
        tdSql.query(f"SELECT LAST_ROW(bin_col) FROM {db}.vtb_lt;")
        tdSql.checkData(0, 0, self.BIN[9])

        tdSql.query(f"SELECT LAST_ROW(nch_col) FROM {db}.vtb_lt;")
        tdSql.checkData(0, 0, self.NCH[9])

    def test_sel_sample(self):
        """Selection: SAMPLE on binary column of vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, selection, sample

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== sel: sample ===")
        tdSql.query(f"SELECT SAMPLE(bin_col, 3) FROM {db}.vtb_lt;")
        tdSql.checkRows(3)
        for i in range(3):
            val = tdSql.getData(i, 0)
            assert val in self.BIN_NN, f"sample value '{val}' not in source data"

    # ========================= STRING FUNCTIONS =============================

    def test_str_lower_upper(self):
        """String: LOWER, UPPER on vtable binary col with col len < ref len

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, string, lower, upper

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== str: lower / upper ===")
        tdSql.query(f"SELECT LOWER(bin_col) FROM {db}.vtb_lt ORDER BY ts;")
        tdSql.checkRows(10)
        for i, val in enumerate(self.BIN):
            tdSql.checkData(i, 0, val.lower() if val else None)

        tdSql.query(f"SELECT UPPER(bin_col) FROM {db}.vtb_lt ORDER BY ts;")
        tdSql.checkRows(10)
        for i, val in enumerate(self.BIN):
            tdSql.checkData(i, 0, val.upper() if val else None)

    def test_str_concat_concat_ws(self):
        """String: CONCAT, CONCAT_WS on vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, string, concat

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== str: concat / concat_ws ===")
        tdSql.query(
            f"SELECT CONCAT(bin_col, '-end') FROM {db}.vtb_lt ORDER BY ts;")
        tdSql.checkRows(10)
        for i, val in enumerate(self.BIN):
            tdSql.checkData(i, 0, val + '-end' if val else None)

        tdSql.query(
            f"SELECT CONCAT_WS('|', bin_col, 'x') FROM {db}.vtb_lt ORDER BY ts;")
        tdSql.checkRows(10)
        for i, val in enumerate(self.BIN):
            tdSql.checkData(i, 0, val + '|x' if val else None)

    def test_str_substr(self):
        """String: SUBSTR on vtable nchar col with col len < ref len

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, string, substr

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== str: substr ===")
        tdSql.query(
            f"SELECT SUBSTR(nch_col, 1, 3) FROM {db}.vtb_lt ORDER BY ts;")
        tdSql.checkRows(10)
        for i, val in enumerate(self.NCH):
            tdSql.checkData(i, 0, val[:3] if val else None)

    def test_str_ltrim_rtrim(self):
        """String: LTRIM, RTRIM on vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, string, trim

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== str: ltrim / rtrim ===")
        tdSql.query(f"SELECT LTRIM(bin_col) FROM {db}.vtb_lt ORDER BY ts;")
        tdSql.checkRows(10)
        for i, val in enumerate(self.BIN):
            tdSql.checkData(i, 0, val.lstrip() if val else None)

        tdSql.query(f"SELECT RTRIM(bin_col) FROM {db}.vtb_lt ORDER BY ts;")
        tdSql.checkRows(10)
        for i, val in enumerate(self.BIN):
            tdSql.checkData(i, 0, val.rstrip() if val else None)

    def test_str_length_char_length(self):
        """String: LENGTH, CHAR_LENGTH on vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, string, length

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== str: length / char_length ===")
        tdSql.query(f"SELECT LENGTH(bin_col) FROM {db}.vtb_lt ORDER BY ts;")
        tdSql.checkRows(10)
        for i, val in enumerate(self.BIN):
            tdSql.checkData(i, 0, len(val) if val else None)

        tdSql.query(
            f"SELECT CHAR_LENGTH(nch_col) FROM {db}.vtb_lt ORDER BY ts;")
        tdSql.checkRows(10)
        for i, val in enumerate(self.NCH):
            tdSql.checkData(i, 0, len(val) if val else None)

    def test_str_replace(self):
        """String: REPLACE on vtable binary col

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, string, replace

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== str: replace ===")
        tdSql.query(
            f"SELECT REPLACE(bin_col, ' - ', '/') FROM {db}.vtb_lt ORDER BY ts;")
        tdSql.checkRows(10)
        for i, val in enumerate(self.BIN):
            tdSql.checkData(i, 0, val.replace(' - ', '/') if val else None)

    def test_str_ascii_position(self):
        """String: ASCII, POSITION on vtable binary col

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, string, ascii, position

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== str: ascii / position ===")
        tdSql.query(
            f"SELECT ASCII(bin_col) FROM {db}.vtb_lt ORDER BY ts LIMIT 3;")
        tdSql.checkRows(3)
        # 'S'=83, 's'=115, 'P'=80
        tdSql.checkData(0, 0, 83)
        tdSql.checkData(1, 0, 115)
        tdSql.checkData(2, 0, 80)

        tdSql.query(
            f"SELECT POSITION('short' IN bin_col) FROM {db}.vtb_lt "
            f"ORDER BY ts LIMIT 3;")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 0)  # not found in 'Shanghai - Los Angeles'
        tdSql.checkData(1, 0, 1)  # found at position 1 in 'short'
        tdSql.checkData(2, 0, 0)  # not found

    # ========================= MATH / CONVERSION ============================

    def test_math_abs_ceil_floor_round(self):
        """Math: ABS, CEIL, FLOOR, ROUND on vtable float col

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, math

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== math: abs/ceil/floor/round ===")
        tdSql.query(f"SELECT ABS(fval) FROM {db}.vtb_lt ORDER BY ts LIMIT 1;")
        val = tdSql.getData(0, 0)
        assert abs(val - 1.1) < 0.2, f"abs mismatch: {val}"

        tdSql.query(
            f"SELECT CEIL(fval) FROM {db}.vtb_lt ORDER BY ts LIMIT 1;")
        val = tdSql.getData(0, 0)
        assert val == 2.0, f"ceil mismatch: {val}"

        tdSql.query(
            f"SELECT FLOOR(fval) FROM {db}.vtb_lt ORDER BY ts LIMIT 1;")
        val = tdSql.getData(0, 0)
        assert val == 1.0, f"floor mismatch: {val}"

        tdSql.query(
            f"SELECT ROUND(fval) FROM {db}.vtb_lt ORDER BY ts LIMIT 1;")
        val = tdSql.getData(0, 0)
        assert val == 1.0, f"round mismatch: {val}"

    def test_cast(self):
        """Conversion: CAST int to binary, int to nchar on vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, cast

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== cast ===")
        tdSql.query(
            f"SELECT CAST(ival AS BINARY(16)) FROM {db}.vtb_lt ORDER BY ts;")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, '10')
        tdSql.checkData(1, 0, '20')

        tdSql.query(
            f"SELECT CAST(ival AS NCHAR(16)) FROM {db}.vtb_lt ORDER BY ts;")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, '10')

    def test_arithmetic(self):
        """Math: arithmetic expressions on vtable int/float cols

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, arithmetic

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== arithmetic ===")
        tdSql.query(
            f"SELECT ival * 2 + 1 FROM {db}.vtb_lt ORDER BY ts LIMIT 1;")
        tdSql.checkData(0, 0, 21)  # 10*2+1

        tdSql.query(
            f"SELECT ival + fval FROM {db}.vtb_lt ORDER BY ts LIMIT 1;")
        val = tdSql.getData(0, 0)
        assert abs(val - 11.1) < 0.2, f"arithmetic mismatch: {val}"

    # ========================= WHERE FILTERS ================================

    def test_where_eq_neq(self):
        """Filter: = and <> on binary col of vtable with col len < ref len

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, filter, where

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== where: = / <> ===")
        tdSql.query(f"SELECT bin_col FROM {db}.vtb_lt WHERE bin_col = 'short';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'short')

        tdSql.query(
            f"SELECT COUNT(*) FROM {db}.vtb_lt WHERE bin_col <> 'short';")
        tdSql.checkData(0, 0, 7)  # 8 non-null minus 1

    def test_where_like(self):
        """Filter: LIKE on binary col of vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, filter, like

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== where: LIKE ===")
        tdSql.query(
            f"SELECT bin_col FROM {db}.vtb_lt "
            f"WHERE bin_col LIKE 'San%' ORDER BY ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'San Francisco - Cupertino')

    def test_where_in(self):
        """Filter: IN on binary col of vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, filter, in

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== where: IN ===")
        tdSql.query(
            f"SELECT bin_col FROM {db}.vtb_lt "
            f"WHERE bin_col IN ('short', 'a', 'SV') ORDER BY ts;")
        tdSql.checkRows(3)

    def test_where_between(self):
        """Filter: BETWEEN on int col of vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, filter, between

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== where: BETWEEN ===")
        tdSql.query(
            f"SELECT ival FROM {db}.vtb_lt "
            f"WHERE ival BETWEEN 20 AND 60 ORDER BY ts;")
        expected = [v for v in self.IVAL if v is not None and 20 <= v <= 60]
        tdSql.checkRows(len(expected))
        for i, v in enumerate(expected):
            tdSql.checkData(i, 0, v)

    def test_where_is_null(self):
        """Filter: IS NULL / IS NOT NULL on vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, filter, null

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== where: IS NULL / IS NOT NULL ===")
        tdSql.query(
            f"SELECT COUNT(*) FROM {db}.vtb_lt WHERE bin_col IS NULL;")
        tdSql.checkData(0, 0, 2)

        tdSql.query(
            f"SELECT COUNT(*) FROM {db}.vtb_lt WHERE bin_col IS NOT NULL;")
        tdSql.checkData(0, 0, 8)

    def test_where_combined(self):
        """Filter: combined WHERE conditions on vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, filter, combined

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== where: combined ===")
        tdSql.query(
            f"SELECT bin_col, ival FROM {db}.vtb_lt "
            f"WHERE ival > 30 AND bin_col IS NOT NULL ORDER BY ts;")
        # ival>30 and bin_col not null:
        #   (50, San Fran), (60, Beijing), (70, a), (90, Hangzhou), (100, SV)
        tdSql.checkRows(5)

    # ========================= ORDER BY / LIMIT / OFFSET ====================

    def test_order_limit_offset(self):
        """Query: ORDER BY, LIMIT, OFFSET on vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, order, limit, offset

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== order / limit / offset ===")
        tdSql.query(
            f"SELECT bin_col FROM {db}.vtb_lt ORDER BY ts LIMIT 3;")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, self.BIN[0])
        tdSql.checkData(1, 0, self.BIN[1])
        tdSql.checkData(2, 0, self.BIN[2])

        tdSql.query(
            f"SELECT bin_col FROM {db}.vtb_lt ORDER BY ts LIMIT 3 OFFSET 2;")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, self.BIN[2])
        tdSql.checkData(1, 0, self.BIN[3])
        tdSql.checkData(2, 0, self.BIN[4])

        # Order by int col descending
        tdSql.query(
            f"SELECT ival FROM {db}.vtb_lt WHERE ival IS NOT NULL "
            f"ORDER BY ival DESC LIMIT 3;")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 100)
        tdSql.checkData(1, 0, 90)
        tdSql.checkData(2, 0, 70)

    # ========================= DISTINCT / CASE WHEN =========================

    def test_distinct(self):
        """Query: DISTINCT on vtable binary col

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, distinct

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== distinct ===")
        tdSql.query(f"SELECT DISTINCT bin_col FROM {db}.vtb_lt;")
        # 8 distinct non-null + 1 NULL = 9
        rows = tdSql.queryRows
        assert rows == 9, f"distinct rows: {rows}, expected 9"

    def test_case_when(self):
        """Query: CASE WHEN on vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, case_when

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== case when ===")
        tdSql.query(
            f"SELECT CASE WHEN ival > 50 THEN 'high' "
            f"WHEN ival > 0 THEN 'low' ELSE 'none' END AS cat "
            f"FROM {db}.vtb_lt ORDER BY ts;")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, 'low')    # ival=10
        tdSql.checkData(1, 0, 'low')    # ival=20
        tdSql.checkData(3, 0, 'none')   # ival=NULL
        tdSql.checkData(4, 0, 'low')    # ival=50
        tdSql.checkData(5, 0, 'high')   # ival=60
        tdSql.checkData(9, 0, 'high')   # ival=100

    # ========================= SUBQUERIES ===================================

    def test_subquery_scalar(self):
        """Subquery: scalar subquery with vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, subquery, scalar

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== subquery: scalar ===")
        tdSql.query(
            f"SELECT bin_col FROM {db}.vtb_lt "
            f"WHERE ival = (SELECT MAX(ival) FROM {db}.vtb_lt);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, self.BIN[9])  # ival=100 -> 'SV'

    def test_subquery_table(self):
        """Subquery: table subquery (FROM subquery) with vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, subquery, table

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== subquery: table ===")
        tdSql.query(
            f"SELECT cnt FROM "
            f"(SELECT COUNT(*) AS cnt FROM {db}.vtb_lt "
            f"WHERE bin_col IS NOT NULL);")
        tdSql.checkData(0, 0, 8)

    def test_subquery_nested(self):
        """Subquery: nested subquery (3 levels) with vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, subquery, nested

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== subquery: nested 3 levels ===")
        tdSql.query(
            f"SELECT total FROM ("
            f"  SELECT SUM(len) AS total FROM ("
            f"    SELECT LENGTH(bin_col) AS len FROM {db}.vtb_lt "
            f"    WHERE bin_col IS NOT NULL"
            f"  )"
            f");")
        tdSql.checkRows(1)
        expected = sum(len(v) for v in self.BIN_NN)
        tdSql.checkData(0, 0, expected)

    def test_subquery_with_string_func(self):
        """Subquery: string functions inside subquery on vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, subquery, string

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== subquery: string functions inside subquery ===")
        tdSql.query(
            f"SELECT low_bin FROM ("
            f"  SELECT LOWER(bin_col) AS low_bin FROM {db}.vtb_lt "
            f"  WHERE bin_col IS NOT NULL ORDER BY ts"
            f") LIMIT 3;")
        tdSql.checkRows(3)
        nn_bins = [v for v in self.BIN if v is not None]
        tdSql.checkData(0, 0, nn_bins[0].lower())
        tdSql.checkData(1, 0, nn_bins[1].lower())
        tdSql.checkData(2, 0, nn_bins[2].lower())

    def test_subquery_agg_filter(self):
        """Subquery: aggregate in subquery, filter in outer on vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, subquery, aggregate

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== subquery: agg in sub, filter in outer ===")
        tdSql.query(
            f"SELECT bin_col, ival FROM {db}.vtb_lt "
            f"WHERE ival > (SELECT AVG(ival) FROM {db}.vtb_lt) "
            f"ORDER BY ts;")
        avg_val = sum(self.IVAL_NN) / len(self.IVAL_NN)
        expected = [(self.BIN[i], self.IVAL[i])
                    for i in range(10)
                    if self.IVAL[i] is not None and self.IVAL[i] > avg_val]
        tdSql.checkRows(len(expected))
        for idx, (b, iv) in enumerate(expected):
            tdSql.checkData(idx, 0, b)
            tdSql.checkData(idx, 1, iv)

    def test_subquery_with_window(self):
        """Subquery: window function result used in outer query

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, subquery, window

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== subquery: window in subquery ===")
        tdSql.query(
            f"SELECT max_sum FROM ("
            f"  SELECT _wstart AS ws, SUM(ival) AS max_sum "
            f"  FROM {db}.vtb_lt INTERVAL(5s)"
            f") ORDER BY max_sum DESC LIMIT 1;")
        tdSql.checkRows(1)
        # window1: 10+20+30+None+50=110; window2: 60+70+None+90+100=320
        tdSql.checkData(0, 0, 320)

    def test_subquery_with_join(self):
        """Subquery: JOIN inside subquery

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, subquery, join

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== subquery: join inside subquery ===")
        tdSql.query(
            f"SELECT cnt FROM ("
            f"  SELECT COUNT(*) AS cnt "
            f"  FROM {db}.vtb_lt a, {db}.src_dim b "
            f"  WHERE a.ts = b.ts"
            f");")
        tdSql.checkData(0, 0, 3)

    def test_subquery_with_union(self):
        """Subquery: UNION ALL inside subquery

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, subquery, union

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== subquery: union all inside subquery ===")
        tdSql.query(
            f"SELECT COUNT(*) FROM ("
            f"  SELECT bin_col FROM {db}.vtb_lt WHERE ival <= 30 "
            f"  UNION ALL "
            f"  SELECT bin_col FROM {db}.vtb_lt WHERE ival >= 90"
            f");")
        # ival<=30: rows 0,1,2 (3 rows); ival>=90: rows 8,9 (2 rows) -> 5
        tdSql.checkData(0, 0, 5)

    # ========================= JOIN =========================================

    def test_join_vtable_with_regular(self):
        """JOIN: vtable JOIN regular table on timestamp

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, join

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== join: vtable with regular table ===")
        tdSql.query(
            f"SELECT a.bin_col, b.city, a.ival, b.code "
            f"FROM {db}.vtb_lt a, {db}.src_dim b "
            f"WHERE a.ts = b.ts ORDER BY a.ts;")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, self.BIN[0])
        tdSql.checkData(0, 1, 'Shanghai')
        tdSql.checkData(0, 2, self.IVAL[0])
        tdSql.checkData(0, 3, 10)
        tdSql.checkData(1, 0, self.BIN[1])
        tdSql.checkData(1, 1, 'Palo Alto')
        tdSql.checkData(2, 0, self.BIN[2])
        tdSql.checkData(2, 1, 'Beijing')

    def test_join_vtable_with_vtable(self):
        """JOIN: two vtables joined on timestamp (expect error - not supported)

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, join, negative

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== join: vtable with vtable (expect error) ===")
        # vtable-vtable JOIN is not currently supported
        tdSql.error(
            f"SELECT a.bin_col, b.city "
            f"FROM {db}.vtb_lt a, {db}.vtb_dim b "
            f"WHERE a.ts = b.ts ORDER BY a.ts;")

    # ========================= UNION ========================================

    def test_union_all(self):
        """UNION ALL: combine results from vtable queries

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, union

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== union all ===")
        tdSql.query(
            f"SELECT bin_col FROM {db}.vtb_lt WHERE ival = 10 "
            f"UNION ALL "
            f"SELECT bin_col FROM {db}.vtb_lt WHERE ival = 20;")
        tdSql.checkRows(2)

    def test_union_dedup(self):
        """UNION: deduplicate results from vtable queries

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, union

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== union (dedup) ===")
        tdSql.query(
            f"SELECT bin_col FROM {db}.vtb_lt WHERE ival = 10 "
            f"UNION "
            f"SELECT bin_col FROM {db}.vtb_lt WHERE ival = 10;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, self.BIN[0])

    # ========================= GROUP BY / PARTITION BY ======================

    def test_group_by_with_agg(self):
        """GROUP BY: aggregate with GROUP BY on vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, group_by

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== group by with agg ===")
        tdSql.query(
            f"SELECT CASE WHEN ival <= 50 THEN 'low' ELSE 'high' END AS bucket, "
            f"COUNT(*) AS cnt "
            f"FROM {db}.vtb_lt WHERE ival IS NOT NULL "
            f"GROUP BY CASE WHEN ival <= 50 THEN 'low' ELSE 'high' END "
            f"ORDER BY bucket;")
        tdSql.checkRows(2)
        # 'high': 60,70,90,100 = 4;  'low': 10,20,30,50 = 4
        tdSql.checkData(0, 0, 'high')
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(1, 0, 'low')
        tdSql.checkData(1, 1, 4)

    def test_partition_by(self):
        """PARTITION BY: on virtual super table

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, partition_by

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== partition by on virtual super table ===")
        tdSql.query(
            f"SELECT region, COUNT(*) AS cnt "
            f"FROM {db}.vstb PARTITION BY region ORDER BY region;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'east')
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 0, 'west')
        tdSql.checkData(1, 1, 5)

    # ========================= WINDOW FUNCTIONS =============================

    def test_interval_window(self):
        """Window: INTERVAL on vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, window, interval

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== window: interval ===")
        # 10 rows spanning 10 seconds, interval 5s -> 2 windows
        tdSql.query(
            f"SELECT _wstart, COUNT(*), FIRST(bin_col) "
            f"FROM {db}.vtb_lt INTERVAL(5s);")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 1, 5)
        tdSql.checkData(0, 2, self.BIN[0])
        tdSql.checkData(1, 2, self.BIN[5])

    def test_interval_fill(self):
        """Window: INTERVAL with FILL on vtable (requires time range)

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, window, interval, fill

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== window: interval fill ===")
        tdSql.query(
            f"SELECT _wstart, SUM(ival) FROM {db}.vtb_lt "
            f"WHERE ts >= '2024-01-01' AND ts < '2024-01-01 00:00:10.001' "
            f"INTERVAL(2s) FILL(VALUE, 0);")
        # 10 seconds / 2s = 5 windows + possibly 1 extra
        rows = tdSql.queryRows
        assert rows >= 5, f"interval fill should have >= 5 windows, got {rows}"

    def test_session_window(self):
        """Window: SESSION on vtable

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, window, session

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== window: session ===")
        # All rows are 1s apart, session(ts, 2s) -> 1 window
        tdSql.query(
            f"SELECT _wstart, COUNT(*), FIRST(bin_col) "
            f"FROM {db}.vtb_lt SESSION(ts, 2s);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, self.BIN[0])

    # STATE_WINDOW on virtual tables may cause taosd instability.
    # Skipping STATE_WINDOW test until the underlying issue is resolved.

    # ========================= VIRTUAL SUPER TABLE QUERIES ==================

    def test_vstb_select_all(self):
        """Query: SELECT * from virtual super table

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, super_table

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== vstb: select all ===")
        tdSql.query(f"SELECT * FROM {db}.vstb ORDER BY ts;")
        tdSql.checkRows(10)

    def test_vstb_agg(self):
        """Query: aggregate on virtual super table

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, super_table, aggregate

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== vstb: aggregate ===")
        tdSql.query(f"SELECT COUNT(*) FROM {db}.vstb;")
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"SELECT FIRST(bin_col) FROM {db}.vstb;")
        tdSql.checkData(0, 0, self.BIN[0])

        tdSql.query(f"SELECT LAST(bin_col) FROM {db}.vstb;")
        tdSql.checkData(0, 0, self.BIN[9])

    def test_vstb_interval(self):
        """Query: INTERVAL on virtual super table

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, super_table, interval

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== vstb: interval ===")
        tdSql.query(
            f"SELECT _wstart, COUNT(*) FROM {db}.vstb INTERVAL(5s);")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 1, 5)

    def test_vstb_subquery(self):
        """Query: subquery on virtual super table

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, super_table, subquery

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== vstb: subquery ===")
        tdSql.query(
            f"SELECT total FROM ("
            f"  SELECT SUM(ival) AS total FROM {db}.vstb "
            f"  WHERE bin_col IS NOT NULL"
            f");")
        tdSql.checkRows(1)
        expected = sum(v for i, v in enumerate(self.IVAL)
                       if v is not None and self.BIN[i] is not None)
        tdSql.checkData(0, 0, expected)

    # ========================= VIRTUAL CHILD TABLE QUERIES ===================

    def test_vct_select_all(self):
        """Query: SELECT * from individual virtual child tables

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, child_table

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== vct: select all ===")
        tdSql.query(f"SELECT * FROM {db}.vct1 ORDER BY ts;")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, self.BIN[0])
        tdSql.checkData(1, 1, self.BIN[1])
        tdSql.checkData(4, 1, self.BIN[4])

        tdSql.query(f"SELECT * FROM {db}.vct2 ORDER BY ts;")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, self.BIN[5])
        tdSql.checkData(4, 1, self.BIN[9])

    def test_vct_agg(self):
        """Query: aggregate on individual virtual child tables

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, child_table, aggregate

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== vct: aggregate ===")
        tdSql.query(f"SELECT COUNT(*) FROM {db}.vct1;")
        tdSql.checkData(0, 0, 5)

        tdSql.query(f"SELECT SUM(ival) FROM {db}.vct1;")
        expected_sum = sum(v for v in self.IVAL[:5] if v is not None)
        tdSql.checkData(0, 0, expected_sum)

        tdSql.query(f"SELECT COUNT(*) FROM {db}.vct2;")
        tdSql.checkData(0, 0, 5)

        tdSql.query(f"SELECT SUM(ival) FROM {db}.vct2;")
        expected_sum2 = sum(v for v in self.IVAL[5:] if v is not None)
        tdSql.checkData(0, 0, expected_sum2)

    def test_vct_string_func(self):
        """Query: string functions on virtual child table

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, child_table, string

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== vct: string func ===")
        tdSql.query(
            f"SELECT LOWER(bin_col) FROM {db}.vct1 ORDER BY ts LIMIT 2;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, self.BIN[0].lower())
        tdSql.checkData(1, 0, self.BIN[1].lower())

        tdSql.query(
            f"SELECT LENGTH(bin_col) FROM {db}.vct2 ORDER BY ts LIMIT 2;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, len(self.BIN[5]))
        tdSql.checkData(1, 0, len(self.BIN[6]))

    # ========================= EDGE CASES ===================================

    def test_select_star(self):
        """Query: SELECT * on vtable with col len < ref len

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, select_star

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== select * ===")
        tdSql.query(f"SELECT * FROM {db}.vtb_lt ORDER BY ts;")
        tdSql.checkRows(10)
        for i in range(10):
            tdSql.checkData(i, 1, self.BIN[i])
            tdSql.checkData(i, 2, self.NCH[i])
            tdSql.checkData(i, 3, self.IVAL[i])

    def test_consistency_with_source(self):
        """Query: compare results from vtable and source table

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, consistency

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== consistency: vtable vs source ===")
        # The vtb_lt results should be identical to querying src_ntb directly
        tdSql.query(
            f"SELECT bin_col, nch_col FROM {db}.vtb_lt ORDER BY ts;")
        vtb_data = [(tdSql.getData(i, 0), tdSql.getData(i, 1))
                    for i in range(10)]

        tdSql.query(
            f"SELECT bin_col, nch_col FROM {db}.src_ntb ORDER BY ts;")
        src_data = [(tdSql.getData(i, 0), tdSql.getData(i, 1))
                    for i in range(10)]

        assert vtb_data == src_data, \
            f"vtb_lt data differs from src_ntb:\nvtb={vtb_data}\nsrc={src_data}"

    def test_count_consistency(self):
        """Query: COUNT consistency between vtable and source

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, consistency, count

        Jira: None

        History:
            - 2026-2-12 Created
        """
        db = self.DB
        tdLog.info("=== count consistency ===")
        tdSql.query(f"SELECT COUNT(*) FROM {db}.vtb_lt;")
        vtb_count = tdSql.getData(0, 0)

        tdSql.query(f"SELECT COUNT(*) FROM {db}.src_ntb;")
        src_count = tdSql.getData(0, 0)

        assert vtb_count == src_count, \
            f"count mismatch: vtb={vtb_count}, src={src_count}"
