###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved
#
#  This file is proprietary and confidential to TAOS Technologies, Inc.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than
#  expressly provided by the written permission of Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""SELECT tag strict-value coverage: virtual normal tables, normal tables, tag-refs.

Dedicated matrix for tag projection on the three tag-bearing surfaces, asserting
EXACT result values (row count, column count, typed cell values incl. NULL/BOOL/
BIGINT boundaries) on every query -- never row-count-only or substring checks.

Surfaces:
  1. virtual normal table owned tags   -- full type matrix (INT/BIGINT/DOUBLE/
     BOOL/NCHAR/VARCHAR/JSON), NULL default, SET TAG visibility + NULL clear
  2. normal (physical) table owned tags -- create-time inline values, ALTER ADD
     TAG over pre-existing rows, SET TAG, SELECT * tag exclusion
  3. tag-refs (FROM <child>.<tag>)      -- projection/follow/DISTINCT/filter,
     multi-source refs, refs onto NULL source tags, mixed owned+ref

Query shapes per surface: projection (any column order), WHERE matrix
(eq/range/IN/AND/OR), GROUP BY + aggregates, DISTINCT, tbname+tag, subquery,
and a final dnode-restart persistence pass over all three surfaces.
"""

import time

import pytest

from new_test_framework.utils import tdLog, tdSql, tdDnodes

DB = "td_vtag_matrix"
TS0 = 1700000000000  # base timestamp for source rows

BIGINT_MAX = 9223372036854775807

# Source stable children: TAGS(city NCHAR(20), code INT, region NCHAR(16))
SRC = {
    'mx_src0': {'city': 'beijing',   'code': 100, 'region': 'east',  'data': [1, 2, 3, 4]},
    'mx_src1': {'city': 'shanghai',  'code': 200, 'region': 'west',  'data': [10, 20, 30]},
    'mx_src2': {'city': 'guangzhou', 'code': 300, 'region': 'south', 'data': [100, 101]},
    # all-NULL tags: refs pointing here must read back NULL per row
    'mx_srcn': {'city': None,        'code': None, 'region': None,  'data': [7, 8]},
}


def _ts(j):
    return TS0 + j * 1000


class TestVtableSelectTagMatrix:
    """SELECT tag strict-value matrix: vntb owned / normal-table owned / tag-refs."""

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------
    def setup_class(cls):
        tdLog.info("=== setup: sources + shared vntb/ntb tag fixtures ===")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB};")
        tdSql.execute(f"CREATE DATABASE {DB} BUFFER 16;")
        tdSql.execute(f"USE {DB};")

        tdSql.execute("CREATE STABLE mx_stb (ts TIMESTAMP, val INT) "
                      "TAGS (city NCHAR(20), code INT, region NCHAR(16));")
        for tbl, info in SRC.items():
            city = "NULL" if info['city'] is None else f"'{info['city']}'"
            code = "NULL" if info['code'] is None else f"{info['code']}"
            region = "NULL" if info['region'] is None else f"'{info['region']}'"
            tdSql.execute(f"CREATE TABLE {tbl} USING mx_stb "
                          f"TAGS ({city}, {code}, {region});")
            for j, v in enumerate(info['data']):
                tdSql.execute(f"INSERT INTO {tbl} VALUES ({_ts(j)}, {v});")

        # virtual normal tables: type matrix / NULL defaults / refs / json
        tdSql.execute("CREATE VTABLE mv_types (ts TIMESTAMP, val INT FROM mx_src0.val) TAGS ("
                      "t_int INT = 7, "
                      "t_big BIGINT = 9223372036854775807, "
                      "t_dbl DOUBLE = 3.5, "
                      "t_bool BOOL = true, "
                      "t_nchar NCHAR(16) = 'hello', "
                      "t_vc VARCHAR(16) = 'world');")
        tdSql.execute("CREATE VTABLE mv_null (ts TIMESTAMP, val INT FROM mx_src0.val) "
                      "TAGS (t_i INT = 1, t_s NCHAR(8) = 'a');")
        tdSql.execute("CREATE VTABLE mv_refs (ts TIMESTAMP, val INT FROM mx_src0.val) TAGS ("
                      "rcity NCHAR(20) FROM mx_src0.city, "
                      "rcode INT FROM mx_src0.code, "
                      "rregion NCHAR(16) FROM mx_src0.region);")
        tdSql.execute("CREATE VTABLE mv_mref (ts TIMESTAMP, val INT FROM mx_src0.val) TAGS ("
                      "c0 NCHAR(20) FROM mx_src0.city, "
                      "c1 NCHAR(20) FROM mx_src1.city, "
                      "c2 NCHAR(20) FROM mx_src2.city);")
        tdSql.execute("CREATE VTABLE mv_refn (ts TIMESTAMP, val INT FROM mx_srcn.val) TAGS ("
                      "ncity NCHAR(20) FROM mx_srcn.city, "
                      "ncode INT FROM mx_srcn.code);")
        tdSql.execute("CREATE VTABLE mv_json (ts TIMESTAMP, val INT FROM mx_src0.val) "
                      "TAGS (j JSON = '{\"k\":\"v\"}');")
        tdSql.execute("CREATE VTABLE mv_mixed (ts TIMESTAMP, val INT FROM mx_src0.val) TAGS ("
                      "lit INT = 100, "
                      "rcity NCHAR(20) FROM mx_src0.city);")

        # normal (physical) tables: inline tag values at CREATE
        tdSql.execute("CREATE TABLE mn_types (ts TIMESTAMP, val INT) TAGS ("
                      "n_int INT = 7, "
                      "n_big BIGINT = 9223372036854775807, "
                      "n_dbl DOUBLE = 3.5, "
                      "n_bool BOOL = true, "
                      "n_nchar NCHAR(16) = 'hello', "
                      "n_vc VARCHAR(16) = 'world');")
        for j, v in enumerate([1, 2, 3]):
            tdSql.execute(f"INSERT INTO mn_types VALUES ({_ts(j)}, {v});")

        # data BEFORE ADD TAG: tag must attach to pre-existing rows as well
        tdSql.execute("CREATE TABLE mn_plain (ts TIMESTAMP, val INT);")
        for j, v in enumerate([5, 6]):
            tdSql.execute(f"INSERT INTO mn_plain VALUES ({_ts(j)}, {v});")
        tdSql.execute("ALTER TABLE mn_plain ADD TAG grp VARCHAR(16);")
        tdLog.info("=== setup complete ===")

    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB};")

    # ------------------------------------------------------------------
    # Helpers -- STRICT value assertions (typed cells, exact rows/cols)
    # ------------------------------------------------------------------
    def _rows_typed(self, sql):
        """Run a query, return rows as a list of typed tuples (None == NULL)."""
        tdSql.query(sql)
        return [tuple(tdSql.getData(i, j) for j in range(tdSql.queryCols))
                for i in range(tdSql.queryRows)]

    @staticmethod
    def _cell_key(cell):
        # py3 cannot order None against scalars; key on (type, repr) so that
        # unordered comparison stays total and typed
        return (type(cell).__name__, repr(cell))

    def _check_exact(self, sql, expected, desc="", ncols=None):
        """Assert the query returns EXACTLY `expected`: same row set (order
        independent), same column count, and identical typed cell values.
        For an empty `expected` the column count cannot be inferred, so pass
        `ncols` explicitly to keep the column-count check active."""
        actual = self._rows_typed(sql)
        want_cols = len(expected[0]) if expected else ncols
        if want_cols is not None:
            assert tdSql.queryCols == want_cols, \
                f"{desc}: expected {want_cols} columns, got {tdSql.queryCols}"
        assert len(actual) == len(expected), \
            f"{desc}: expected {len(expected)} rows, got {len(actual)} (actual={actual})"
        a = sorted(actual, key=lambda r: tuple(self._cell_key(c) for c in r))
        e = sorted(expected, key=lambda r: tuple(self._cell_key(c) for c in r))
        assert a == e, f"{desc}:\n  expected {e}\n  actual   {a}"
        tdLog.info(f"  PASS: {desc}")

    # ==================================================================
    # 1. VNTB OWNED TAGS -- type matrix
    # ==================================================================
    def test_vntb_owned_type_matrix_projection(self):
        """vntb owned: every supported tag type projects its exact stored value.

        Asserts every tag of mv_types (INT/BIGINT/DOUBLE/BOOL/NCHAR/VARCHAR)
        projects its exact create-time value on each of the 4 data rows, both
        in the full projection and in per-type single projections.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, select, types

        Jira: None

        History:
            - 2026-09-16 Created -- strict typed matrix (INT/BIGINT/DOUBLE/BOOL/NCHAR/VARCHAR).
        """
        tdSql.execute(f"USE {DB};")
        vals = SRC['mx_src0']['data']
        expected = [(7, BIGINT_MAX, 3.5, True, 'hello', 'world', v) for v in vals]
        self._check_exact(
            "SELECT t_int, t_big, t_dbl, t_bool, t_nchar, t_vc, val FROM mv_types;",
            expected, "vntb: full type matrix projection")
        # single-type projections, exact per-row
        self._check_exact("SELECT t_big FROM mv_types;",
                          [(BIGINT_MAX,) for _ in vals], "vntb: BIGINT max per row")
        self._check_exact("SELECT t_bool FROM mv_types;",
                          [(True,) for _ in vals], "vntb: BOOL true per row")
        self._check_exact("SELECT t_dbl FROM mv_types;",
                          [(3.5,) for _ in vals], "vntb: DOUBLE per row")
        self._check_exact("SELECT t_nchar, t_vc FROM mv_types;",
                          [('hello', 'world') for _ in vals], "vntb: NCHAR+VARCHAR per row")

    def test_vntb_owned_projection_order(self):
        """vntb owned: tag/data mix projects correctly in any column order.

        Queries mv_types with tag and data columns in different positions --
        (val, tag), (tag, ts, val) and (tag, val, tag) -- and asserts the
        exact typed value of every cell in each ordering.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, select

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"USE {DB};")
        vals = SRC['mx_src0']['data']
        self._check_exact("SELECT val, t_int FROM mv_types;",
                          [(v, 7) for v in vals], "vntb: (val, tag)")
        self._check_exact("SELECT t_int, CAST(ts AS BIGINT), val FROM mv_types;",
                          [(7, _ts(j), v) for j, v in enumerate(vals)],
                          "vntb: (tag, ts, val)")
        self._check_exact("SELECT t_nchar, val, t_bool FROM mv_types;",
                          [('hello', v, True) for v in vals], "vntb: (tag, val, tag)")

    def test_vntb_owned_filter_matrix(self):
        """vntb owned: WHERE matrix on INT/DOUBLE/NCHAR tags returns exact rows.

        Runs equality, inequality, DOUBLE range, IN hit/miss, tag AND/OR data
        column and BOOL+BIGINT predicates on mv_types, asserting the exact
        set of val rows returned by each filter.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, select, filter

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"USE {DB};")
        vals = SRC['mx_src0']['data']
        self._check_exact("SELECT val FROM mv_types WHERE t_int = 7;",
                          [(v,) for v in vals], "vntb: tag eq hits all")
        self._check_exact("SELECT val FROM mv_types WHERE t_int != 7;",
                          [], "vntb: tag neq no rows", ncols=1)
        self._check_exact("SELECT val FROM mv_types WHERE t_dbl > 3.0 AND t_dbl <= 3.5;",
                          [(v,) for v in vals], "vntb: DOUBLE range")
        self._check_exact("SELECT val FROM mv_types WHERE t_nchar = 'hello';",
                          [(v,) for v in vals], "vntb: NCHAR eq")
        self._check_exact("SELECT val FROM mv_types WHERE t_int IN (5, 7);",
                          [(v,) for v in vals], "vntb: IN hit")
        self._check_exact("SELECT val FROM mv_types WHERE t_int IN (5, 6);",
                          [], "vntb: IN miss", ncols=1)
        self._check_exact("SELECT val FROM mv_types WHERE t_int = 7 AND val >= 3;",
                          [(3,), (4,)], "vntb: tag AND data")
        self._check_exact("SELECT val FROM mv_types WHERE t_int = 8 OR val = 1;",
                          [(1,)], "vntb: tag OR data")
        self._check_exact("SELECT val FROM mv_types WHERE t_bool = true AND t_big > 0;",
                          [(v,) for v in vals], "vntb: BOOL+BIGINT filter")

    def test_vntb_owned_group_agg(self):
        """vntb owned: GROUP BY tag and aggregates carry the exact tag value.

        Groups mv_types by t_int and asserts the exact aggregate row
        (COUNT/SUM/MIN/MAX/AVG) next to the tag value, then checks DISTINCT
        over two tags and COUNT(*) with a tag filter.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, select, aggregate

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"USE {DB};")
        vals = SRC['mx_src0']['data']
        self._check_exact("SELECT t_int, COUNT(*), SUM(val), MIN(val), MAX(val), AVG(val) "
                          "FROM mv_types GROUP BY t_int;",
                          [(7, 4, sum(vals), min(vals), max(vals), 2.5)],
                          "vntb: GROUP BY tag with agg row")
        self._check_exact("SELECT DISTINCT t_int, t_nchar FROM mv_types;",
                          [(7, 'hello')], "vntb: DISTINCT multi-tag")
        self._check_exact("SELECT COUNT(*) FROM mv_types WHERE t_int = 7;",
                          [(4,)], "vntb: COUNT with tag filter")

    def test_vntb_owned_set_tag_null_and_reassign(self):
        """vntb owned: SET TAG reassigns, NULL-clears and reassigns after NULL.

        SET TAG reassigns values exactly; SET TAG NULL clears to NULL;
        reassign after NULL restores a value. (CREATE VTABLE itself always
        requires `= literal` or FROM -- no create-time NULL.) Checked on
        mv_null via exact projections and DISTINCT.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, select, null, set_tag

        Jira: None

        History:
            - 2026-09-16 Created -- also guards the SET TAG apply crash regression.
        """
        tdSql.execute(f"USE {DB};")
        vals = SRC['mx_src0']['data']
        self._check_exact("SELECT t_i, t_s FROM mv_null;",
                          [(1, 'a') for _ in vals], "vntb: initial inline values")
        tdSql.execute("ALTER TABLE mv_null SET TAG t_i = 42;")
        tdSql.execute("ALTER TABLE mv_null SET TAG t_s = 'xx';")
        self._check_exact("SELECT t_i, t_s FROM mv_null;",
                          [(42, 'xx') for _ in vals], "vntb: SET TAG values visible")
        tdSql.execute("ALTER TABLE mv_null SET TAG t_i = NULL;")
        self._check_exact("SELECT t_i, t_s FROM mv_null;",
                          [(None, 'xx') for _ in vals], "vntb: SET TAG NULL clears value")
        self._check_exact("SELECT DISTINCT t_i FROM mv_null;",
                          [(None,)], "vntb: DISTINCT cleared tag reads NULL")
        tdSql.execute("ALTER TABLE mv_null SET TAG t_i = 55;")
        self._check_exact("SELECT DISTINCT t_i FROM mv_null;",
                          [(55,)], "vntb: reassign after NULL")

    def test_vntb_json_tag_select(self):
        """vntb owned: JSON tag projects its exact stored JSON text.

        Asserts DISTINCT over the JSON tag of mv_json returns the exact stored
        JSON text '{"k":"v"}', and that the table still returns its full data
        row count.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, select, json

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"USE {DB};")
        vals = SRC['mx_src0']['data']
        self._check_exact("SELECT DISTINCT j FROM mv_json;",
                          [('{"k":"v"}',)], "vntb: DISTINCT JSON tag exact text")
        self._check_exact("SELECT COUNT(*) FROM mv_json;",
                          [(len(vals),)], "vntb: json-tagged table data intact")

    def test_vntb_tbname_with_tag(self):
        """vntb owned: tbname projects together with the exact tag value.

        Selects tbname and t_int from mv_types and asserts every row carries
        the exact table name 'mv_types' and the exact tag value 7.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, select, tbname

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"USE {DB};")
        vals = SRC['mx_src0']['data']
        self._check_exact("SELECT tbname, t_int FROM mv_types;",
                          [('mv_types', 7) for _ in vals], "vntb: tbname+tag per row")

    def test_vntb_tag_in_subquery(self):
        """vntb owned: tag projects through a subquery with exact values.

        Wraps mv_types in a subquery selecting ts, val and t_int, and asserts
        the outer projection still returns the exact tag value 7 alongside
        each val row.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, select, subquery

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"USE {DB};")
        vals = SRC['mx_src0']['data']
        self._check_exact("SELECT t_int, val FROM (SELECT ts, val, t_int FROM mv_types);",
                          [(7, v) for v in vals], "vntb: tag through subquery")

    # ==================================================================
    # 2. NORMAL (PHYSICAL) TABLE OWNED TAGS
    # ==================================================================
    def test_ntb_inline_type_matrix_projection(self):
        """ntb owned: create-time inline tag values of every type project exactly.

        Asserts every tag of mn_types (INT/BIGINT/DOUBLE/BOOL/NCHAR/VARCHAR)
        projects its exact create-time value on each of the 3 data rows, both
        in the full projection and in per-type single projections.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, select, types

        Jira: None

        History:
            - 2026-09-16 Created -- strict typed matrix on physical normal tables.
        """
        tdSql.execute(f"USE {DB};")
        vals = [1, 2, 3]
        expected = [(7, BIGINT_MAX, 3.5, True, 'hello', 'world', v) for v in vals]
        self._check_exact(
            "SELECT n_int, n_big, n_dbl, n_bool, n_nchar, n_vc, val FROM mn_types;",
            expected, "ntb: full type matrix projection")
        self._check_exact("SELECT n_big FROM mn_types;",
                          [(BIGINT_MAX,) for _ in vals], "ntb: BIGINT max per row")
        self._check_exact("SELECT n_bool FROM mn_types;",
                          [(True,) for _ in vals], "ntb: BOOL true per row")
        self._check_exact("SELECT n_nchar, n_vc FROM mn_types;",
                          [('hello', 'world') for _ in vals], "ntb: NCHAR+VARCHAR per row")

    def test_ntb_add_tag_over_existing_rows(self):
        """ntb owned: ADD TAG over pre-existing rows yields NULL, then SET TAG applies.

        On mn_plain (rows inserted before the ALTER) the added tag grp reads
        back NULL on every old row; after SET TAG grp = 'g9' every
        already-stored row reads back the exact new value.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, select, alter

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"USE {DB};")
        vals = [5, 6]
        self._check_exact("SELECT grp, val FROM mn_plain;",
                          [(None, 5), (None, 6)], "ntb: ADD TAG default NULL on old rows")
        tdSql.execute("ALTER TABLE mn_plain SET TAG grp = 'g9';")
        self._check_exact("SELECT grp, val FROM mn_plain;",
                          [('g9', 5), ('g9', 6)], "ntb: SET TAG reaches pre-existing rows")

    def test_ntb_filter_group_matrix(self):
        """ntb owned: WHERE/GROUP BY/DISTINCT/aggregate matrix returns exact values.

        Runs tag equality, multi-tag AND and a miss filter on mn_types, then
        asserts the exact GROUP BY n_vc aggregate row, the DISTINCT two-tag
        row and COUNT(*) filtered on the BIGINT boundary value.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, select, filter, aggregate

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"USE {DB};")
        vals = [1, 2, 3]
        self._check_exact("SELECT val FROM mn_types WHERE n_int = 7;",
                          [(v,) for v in vals], "ntb: tag eq filter")
        self._check_exact("SELECT val FROM mn_types WHERE n_nchar = 'hello' AND n_dbl = 3.5;",
                          [(v,) for v in vals], "ntb: multi-tag AND filter")
        self._check_exact("SELECT val FROM mn_types WHERE n_int > 100;",
                          [], "ntb: tag filter miss", ncols=1)
        self._check_exact("SELECT n_vc, COUNT(*), SUM(val), MIN(val), MAX(val), AVG(val) "
                          "FROM mn_types GROUP BY n_vc;",
                          [('world', 3, 6, 1, 3, 2.0)], "ntb: GROUP BY tag agg row")
        self._check_exact("SELECT DISTINCT n_int, n_bool FROM mn_types;",
                          [(7, True)], "ntb: DISTINCT multi-tag")
        self._check_exact("SELECT COUNT(*) FROM mn_types WHERE n_big = 9223372036854775807;",
                          [(3,)], "ntb: BIGINT boundary filter")

    def test_ntb_set_tag_update_visibility(self):
        """ntb owned: SET TAG updates are visible immediately with exact new values.

        Creates mn_vis with tag k = 1, applies two SET TAG updates, and
        asserts each new value is visible at once with exact results in a
        plain projection, a DISTINCT query and a WHERE filter on the new value.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, select, set_tag

        Jira: None

        History:
            - 2026-09-16 Created -- also guards the SET TAG apply crash regression.
        """
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE TABLE mn_vis (ts TIMESTAMP, val INT) TAGS (k INT = 1);")
        tdSql.execute(f"INSERT INTO mn_vis VALUES ({_ts(0)}, 9);")
        self._check_exact("SELECT k, val FROM mn_vis;", [(1, 9)], "ntb: initial value")
        tdSql.execute("ALTER TABLE mn_vis SET TAG k = 2;")
        self._check_exact("SELECT k, val FROM mn_vis;", [(2, 9)], "ntb: updated value")
        tdSql.execute("ALTER TABLE mn_vis SET TAG k = 3;")
        self._check_exact("SELECT DISTINCT k FROM mn_vis;", [(3,)], "ntb: second update")
        self._check_exact("SELECT val FROM mn_vis WHERE k = 3;", [(9,)], "ntb: filter on new value")

    def test_ntb_select_star_excludes_tags(self):
        """ntb owned: SELECT * keeps plain-column semantics (tags excluded).

        Asserts SELECT * on mn_types returns exactly 3 rows x 2 columns (ts
        and val only, no tag columns), then verifies the exact ts/val data
        through a deterministic CAST projection.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, select

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"USE {DB};")
        tdSql.query("SELECT * FROM mn_types;")
        assert tdSql.queryRows == 3 and tdSql.queryCols == 2, \
            f"SELECT * must be exactly 3 rows x 2 cols, " \
            f"got {tdSql.queryRows} x {tdSql.queryCols}"
        # strict data check through the deterministic projection
        self._check_exact("SELECT CAST(ts AS BIGINT), val FROM mn_types;",
                          [(_ts(j), v) for j, v in enumerate([1, 2, 3])],
                          "ntb: SELECT * data exact via CAST projection")
        tdLog.info("  PASS: ntb SELECT * excludes tags, data exact")

    # ==================================================================
    # 3. TAG-REFS (FROM <super-table child>.<tag>)
    # ==================================================================
    def test_ref_projection_matrix(self):
        """tag-ref: refs project the exact source tag values, single and multi source.

        Asserts single-source refs on mv_refs read the exact mx_src0 tag
        values, multi-source refs on mv_mref read beijing/shanghai/guangzhou,
        and mixed owned+ref (mv_mixed) and ref+data projections return exact
        typed cells.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, tag_ref, select

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"USE {DB};")
        vals = SRC['mx_src0']['data']
        self._check_exact("SELECT rcity, rcode, rregion FROM mv_refs;",
                          [('beijing', 100, 'east') for _ in vals],
                          "ref: single-source ref projection")
        self._check_exact("SELECT c0, c1, c2 FROM mv_mref;",
                          [('beijing', 'shanghai', 'guangzhou') for _ in vals],
                          "ref: multi-source ref projection")
        self._check_exact("SELECT lit, rcity FROM mv_mixed;",
                          [(100, 'beijing') for _ in vals],
                          "ref: owned+ref mixed projection")
        self._check_exact("SELECT rcity, val FROM mv_refs;",
                          [('beijing', v) for v in vals], "ref: tag+data projection")

    def test_ref_follows_source_set_tag(self):
        """tag-ref: SET TAG on the SOURCE child is followed by the ref immediately.

        Applies SET TAG on mx_src0 (city, then code) and asserts the refs on
        mv_refs immediately read the new exact values via DISTINCT, then
        restores the original source tag values and re-checks them.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, tag_ref, set_tag

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"USE {DB};")
        vals = SRC['mx_src0']['data']
        try:
            tdSql.execute("ALTER TABLE mx_src0 SET TAG city = 'tianjin';")
            self._check_exact("SELECT DISTINCT rcity FROM mv_refs;",
                              [('tianjin',)], "ref: follows source SET TAG")
            tdSql.execute("ALTER TABLE mx_src0 SET TAG code = 999;")
            self._check_exact("SELECT DISTINCT rcode FROM mv_refs;",
                              [(999,)], "ref: follows source INT SET TAG")
        finally:
            tdSql.execute("ALTER TABLE mx_src0 SET TAG city = 'beijing';")
            tdSql.execute("ALTER TABLE mx_src0 SET TAG code = 100;")
        self._check_exact("SELECT DISTINCT rcity, rcode FROM mv_refs;",
                          [('beijing', 100)], "ref: restored source values")

    def test_ref_null_source_tags(self):
        """tag-ref: refs onto NULL source tags read back NULL per row.

        Selects the refs of mv_refn, whose source child mx_srcn has all-NULL
        tags, and asserts every data row reads back (NULL, NULL), including
        the DISTINCT result.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, tag_ref, null

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"USE {DB};")
        vals = SRC['mx_srcn']['data']
        self._check_exact("SELECT ncity, ncode FROM mv_refn;",
                          [(None, None) for _ in vals], "ref: NULL source tags per row")
        self._check_exact("SELECT DISTINCT ncity, ncode FROM mv_refn;",
                          [(None, None)], "ref: DISTINCT NULL source tags")

    def test_ref_filter_group_matrix(self):
        """tag-ref: WHERE/DISTINCT/aggregate on ref tags return exact values.

        Runs INT/NCHAR equality, a miss, IN and BETWEEN filters on the refs of
        mv_refs, then asserts the exact DISTINCT multi-ref row and the exact
        GROUP BY rcity aggregate row carrying the ref tag value.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, tag_ref, filter, aggregate

        Jira: None

        History:
            - 2026-09-16 Created
        """
        tdSql.execute(f"USE {DB};")
        vals = SRC['mx_src0']['data']
        self._check_exact("SELECT val FROM mv_refs WHERE rcode = 100;",
                          [(v,) for v in vals], "ref: INT eq filter")
        self._check_exact("SELECT val FROM mv_refs WHERE rcity = 'beijing';",
                          [(v,) for v in vals], "ref: NCHAR eq filter")
        self._check_exact("SELECT val FROM mv_refs WHERE rcode > 150;",
                          [], "ref: filter miss", ncols=1)
        self._check_exact("SELECT val FROM mv_refs WHERE rcity IN ('beijing', 'shanghai');",
                          [(v,) for v in vals], "ref: IN filter")
        self._check_exact("SELECT DISTINCT rcity, rcode, rregion FROM mv_refs;",
                          [('beijing', 100, 'east')], "ref: DISTINCT multi-ref")
        self._check_exact("SELECT rcity, COUNT(*), SUM(val) FROM mv_refs GROUP BY rcity;",
                          [('beijing', 4, sum(vals))], "ref: GROUP BY ref agg row")
        self._check_exact("SELECT COUNT(*) FROM mv_refs WHERE rcode BETWEEN 100 AND 200;",
                          [(4,)], "ref: BETWEEN filter count")

    # ==================================================================
    # 4. RESTART PERSISTENCE -- all three surfaces, strict values
    # ==================================================================
    def _restart_dnode(self):
        try:
            tdDnodes.stop(1)
            tdDnodes.start(1)
            time.sleep(3)
        except Exception as e:
            pytest.fail(f"[persistence] dnode restart failed: {e}")

    def test_restart_tag_values_persist(self):
        """Restart: owned tags, SET TAG values and refs survive a dnode restart.

        Stages fresh SET TAG values right before restarting the dnode, then
        asserts exact values on all three surfaces: vntb/ntb create-time owned
        tags, vntb/ntb SET TAG values, tag-ref resolution and NULL source refs
        (values are committed in meta, not memory).

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, normal_table, tag, tag_ref, restart, persistence

        Jira: None

        History:
            - 2026-09-16 Created -- guards the SET TAG txn-abort -> NULL regression.
        """
        tdSql.execute(f"USE {DB};")
        # stage fresh values right before the restart (self-contained: do not rely on
        # values left behind by earlier test methods, so this case also passes standalone)
        tdSql.execute("ALTER TABLE mv_null SET TAG t_i = 77;")
        tdSql.execute("ALTER TABLE mv_null SET TAG t_s = 'xx';")
        tdSql.execute("ALTER TABLE mn_plain SET TAG grp = 'persisted';")
        self._restart_dnode()
        tdSql.execute(f"USE {DB};")
        vals = SRC['mx_src0']['data']
        self._check_exact("SELECT DISTINCT t_int, t_big, t_dbl, t_bool, t_nchar, t_vc "
                          "FROM mv_types;",
                          [(7, BIGINT_MAX, 3.5, True, 'hello', 'world')],
                          "restart: vntb create-time owned tags persist")
        self._check_exact("SELECT t_i, t_s FROM mv_null;",
                          [(77, 'xx') for _ in vals],
                          "restart: vntb SET TAG value persists")
        self._check_exact("SELECT DISTINCT n_int, n_dbl, n_nchar FROM mn_types;",
                          [(7, 3.5, 'hello')], "restart: ntb create-time tags persist")
        self._check_exact("SELECT grp, val FROM mn_plain;",
                          [('persisted', 5), ('persisted', 6)],
                          "restart: ntb SET TAG value persists")
        self._check_exact("SELECT DISTINCT rcity, rcode FROM mv_refs;",
                          [('beijing', 100)], "restart: tag-ref resolution persists")
        self._check_exact("SELECT DISTINCT ncity FROM mv_refn;",
                          [(None,)], "restart: NULL source ref persists")
