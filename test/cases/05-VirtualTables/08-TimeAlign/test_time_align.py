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
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
"""Matrix-driven system tests for TABLE(TIME_ALIGN(...)) (phase 1: PREV).

Instead of one query per case, the suite builds a Cartesian matrix and drives
the product through a small Python reference model of PREV:

  syntax variants  x  table shapes / data distributions  x  query shapes

- Syntax variants: default arguments, explicit default, '*:PREV:_a',
  TRUE + '*:PREV:_a', single column forms ('col', 'col::suffix',
  'col:method:'), TRUE + single column, multiple definitions and '*' plus an
  extra definition.
- Table shapes / distributions: virtual normal table over dense, interior-NULL,
  leading-NULL, single-row, all-NULL-column and empty sources; a multi-source
  virtual table with disjoint source timelines; a virtual child table and two
  virtual super tables whose children overlap or not (per-child PREV reset).
- Query shapes: full projection, WHERE on aligned columns, ORDER BY + LIMIT +
  OFFSET, aggregates, INTERVAL windows, expressions over aligned columns,
  output_raw raw columns and tag filtering/grouping.

Every generated query's result set is compared value by value with the model,
including NULL propagation and per-child resets.

A separate case covers all TDengine user-creatable column types (BOOL through
DECIMAL(38,x), including TIMESTAMP, BINARY/NCHAR, VARBINARY and GEOMETRY).
Tags (JSON included) are not interpolable by design; that is asserted in the
negative cases.
"""

import taos

from new_test_framework.utils import TDSql, tdLog, tdSql

DB = "ta_align"
# 2026-01-01 00:00:00 UTC in epoch milliseconds; absolute timestamps keep the
# assertions timezone-independent.
BASE_TS = 1767225600000
WINDOW_MS = 2000

PAR_SYNTAX_ERROR = 0x80002600
PAR_TABLE_NOT_EXIST = 0x80002603
PAR_INVALID_COLUMN = 0x80002602
PAR_PERMISSION_DENIED = 0x80002644
PAR_DUPLICATED_COLUMN = 0x8000263C
PAR_NOT_ALLOWED_FUNC = 0x8000264F
PAR_INVALID_COLUMNS_NUM = 0x8000263F
PAR_INVALID_TABLE_TYPE = 0x8000268E
PAR_INVALID_OPTR_USAGE = 0x80002667
PAR_FUNC_NOT_EXIST_ORDERBY = 0x800026AA
MND_FUNC_NOT_EXIST = 0x80000374
VTABLE_TOPIC_REJECT = 0x80002639

# 1200-byte value: wider than the declared varchar(4) but fits the varchar(2048) source.
WIDE_VC = "b" * 1200


def ts(offset):
    return BASE_TS + offset


# --------------------------------------------------------------------------
# Reference data.  Offsets are milliseconds from BASE_TS; a missing column or
# a None value is SQL NULL.
# --------------------------------------------------------------------------
COLS3 = ("voltage", "current", "status")
COLS2 = ("voltage", "current")
COLS1 = ("voltage",)

SOURCE_SCHEMAS = {
    "t_gaps": [("voltage", "double"), ("current", "double"), ("status", "int")],
    "t_head": [("voltage", "double"), ("current", "double"), ("status", "int")],
    "t_dense": [("voltage", "double"), ("current", "double"), ("status", "int")],
    "t_single": [("voltage", "double"), ("current", "double"), ("status", "int")],
    "t_nullcol": [("voltage", "double"), ("current", "double"), ("status", "int")],
    "s_v": [("voltage", "double")],
    "s_c": [("current", "double")],
    "s_s": [("status", "int")],
    "t_ntag_src": [("voltage", "double")],
    "t_empty": [("voltage", "double")],
}

STABLE_NAME = "st_src"
STABLE_SCHEMA = [("voltage", "double"), ("current", "double")]
STABLE_TAG = "site int"
CHILDREN = {"d1": 1, "d2": 2, "o1": 1, "o2": 2, "o3": 3}

SOURCE_ROWS = {
    # interior NULL gaps in every column, at different rows
    "t_gaps": {
        0: {"voltage": 1, "current": 10, "status": 1},
        1000: {"voltage": None, "current": 20, "status": None},
        2000: {"voltage": 3, "current": None, "status": 3},
        3000: {"voltage": None, "current": None, "status": 4},
        4000: {"voltage": 5, "current": 50, "status": None},
        5000: {"voltage": None, "current": 60, "status": 6},
    },
    # leading NULLs: PREV must emit NULL before the first non-NULL value
    "t_head": {
        0: {"voltage": None, "current": None, "status": 7},
        1000: {"voltage": None, "current": 20, "status": None},
        2000: {"voltage": 3, "current": None, "status": None},
        3000: {"voltage": 4, "current": 40, "status": 9},
    },
    # no NULLs at all
    "t_dense": {
        0: {"voltage": 10, "current": 100, "status": 1},
        1000: {"voltage": 20, "current": 200, "status": 2},
        2000: {"voltage": 30, "current": 300, "status": 3},
        3000: {"voltage": 40, "current": 400, "status": 4},
    },
    # single row
    "t_single": {
        0: {"voltage": 7, "current": 70, "status": 7},
    },
    # one column is NULL in every row
    "t_nullcol": {
        0: {"voltage": 1, "current": None, "status": 1},
        1000: {"voltage": 2, "current": None, "status": 2},
        2000: {"voltage": None, "current": None, "status": 3},
    },
    # multi-source virtual table: each column has its own source timeline
    "s_v": {0: {"voltage": 10}, 2000: {"voltage": 20}, 5000: {"voltage": None}, 6000: {"voltage": 30}},
    "s_c": {1000: {"current": 100}, 2000: {"current": 200}, 4000: {"current": 300}},
    "s_s": {0: {"status": 1}, 3000: {"status": 3}},
    # vstable children, disjoint timelines
    "d1": {0: {"voltage": 10, "current": 100}, 1000: {"voltage": None, "current": 200}},
    "d2": {2000: {"voltage": 20, "current": None}, 3000: {"voltage": 30, "current": 300}},
    # vstable children, overlapping timelines; o2 starts with a NULL current
    "o1": {0: {"voltage": 10, "current": 100}, 1000: {"voltage": None, "current": 200}},
    "o2": {0: {"voltage": 20, "current": None}, 1000: {"voltage": 30, "current": 300}},
    "o3": {1000: {"voltage": 40, "current": None}, 4000: {"voltage": 50, "current": 500}},
    "t_ntag_src": {0: {"voltage": 10}, 1000: {"voltage": 20}},
    "t_empty": {},
}


def _columns_of(table, cols):
    return [(col, table, col) for col in cols]


def _normal_shape(name, table, cols, **extra):
    shape = {
        "kind": "vtable",
        "name": name,
        "columns": list(cols),
        "children": [{"site": None, "columns": _columns_of(table, cols)}],
    }
    shape.update(extra)
    return shape


def _vstable_shape(name, children):
    return {
        "kind": "vstable",
        "name": name,
        "columns": [col for col, _, _ in children[0][1]],
        "children": [{"site": site, "columns": list(cols)} for site, cols in children],
    }


def _child_shape(name, parent, table, site, cols):
    return {
        "kind": "vtable",
        "name": name,
        "parent": parent,
        "columns": list(cols),
        "children": [{"site": site, "columns": _columns_of(table, cols)}],
    }


SHAPES = {
    "vt_gaps": _normal_shape("vt_gaps", "t_gaps", COLS3),
    "vt_head": _normal_shape("vt_head", "t_head", COLS3),
    "vt_dense": _normal_shape("vt_dense", "t_dense", COLS3),
    "vt_single": _normal_shape("vt_single", "t_single", COLS3),
    "vt_nullcol": _normal_shape("vt_nullcol", "t_nullcol", COLS3),
    "vt_multi": _normal_shape(
        "vt_multi",
        None,
        COLS3,
    ),
    "vt_ntag": _normal_shape(
        "vt_ntag", "t_ntag_src", COLS1, ddl_suffix=" TAGS (name nchar(16) = 'alpha', cnt int = 7)"
    ),
    "vt_empty": _normal_shape("vt_empty", "t_empty", COLS1),
    "vd1": _child_shape("vd1", "vst_disjoint", "d1", 1, COLS2),
    "vd2": _child_shape("vd2", "vst_disjoint", "d2", 2, COLS2),
    "vo1": _child_shape("vo1", "vst_overlap", "o1", 1, COLS2),
    "vo2": _child_shape("vo2", "vst_overlap", "o2", 2, COLS2),
    "vo3": _child_shape("vo3", "vst_overlap", "o3", 3, COLS2),
    "vst_disjoint": _vstable_shape(
        "vst_disjoint",
        [(1, _columns_of("d1", COLS2)), (2, _columns_of("d2", COLS2))],
    ),
    "vst_overlap": _vstable_shape(
        "vst_overlap",
        [(1, _columns_of("o1", COLS2)), (2, _columns_of("o2", COLS2)), (3, _columns_of("o3", COLS2))],
    ),
}

# vt_multi maps each virtual column to a different source table.
SHAPES["vt_multi"]["children"] = [{
    "site": None,
    "columns": [("voltage", "s_v", "voltage"), ("current", "s_c", "current"), ("status", "s_s", "status")],
}]


# --------------------------------------------------------------------------
# Reference model
# --------------------------------------------------------------------------
def _source_type(table, col):
    schema = SOURCE_SCHEMAS.get(table, STABLE_SCHEMA)
    for name, typ in schema:
        if name == col:
            return typ
    raise KeyError(f"{table}.{col}")


def _sql_value(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _source_raw(table, offset, col):
    row = SOURCE_ROWS[table].get(offset)
    return None if row is None else row.get(col)


def _shape_has_site(shape):
    return any(child["site"] is not None for child in shape["children"])


def parse_definition(def_text, column_names):
    """Mirror of the parser's column-definition expansion for positive forms."""
    text = def_text if def_text else "*:PREV:"
    entries = []
    for raw in text.split(";"):
        segment = raw.strip()
        if not segment:
            continue
        parts = segment.split(":")
        name = parts[0].strip().strip('`"')
        suffix = parts[2].strip() if len(parts) > 2 else ""
        if name == "*":
            entries.extend((col, col + suffix) for col in column_names)
        else:
            entries.append((name, name + suffix))
    return entries


def build_frame(shape, referenced_cols, output_raw):
    """Per-row raw and PREV-aligned values for every child of a shape."""
    frame = []
    for child in shape["children"]:
        used = child["columns"] if output_raw else [
            column for column in child["columns"] if column[0] in referenced_cols
        ]
        offsets = sorted({offset for _, source, _ in used for offset in SOURCE_ROWS[source]})
        aligned = {}
        for vcol in referenced_cols:
            mapping = next(column for column in child["columns"] if column[0] == vcol)
            values = []
            last = None
            for offset in offsets:
                value = _source_raw(mapping[1], offset, mapping[2])
                if value is not None:
                    last = value
                values.append(last)
            aligned[vcol] = values
        for index, offset in enumerate(offsets):
            frame.append({
                "site": child["site"],
                "ts": ts(offset),
                "raw": {vcol: _source_raw(src, offset, srccol) for vcol, src, srccol in child["columns"]},
                "align": {vcol: aligned[vcol][index] for vcol in referenced_cols},
            })
    frame.sort(key=lambda row: ((row["site"] if row["site"] is not None else 0), row["ts"]))
    return frame


def natural_rows(shape, entries, output_raw):
    referenced = set(shape["columns"]) if output_raw else {src for src, _ in entries}
    frame = build_frame(shape, referenced, output_raw)
    rows = []
    for row in frame:
        out = [row["ts"]]
        if output_raw:
            out.extend(row["raw"][col] for col in shape["columns"])
        out.extend(row["align"][src] for src, _ in entries)
        if output_raw and shape["kind"] == "vstable":
            out.append(row["site"])
        rows.append(out)
    return rows


def _assert_query(sql, expected_rows, expected_cols=None):
    tdLog.info(sql)
    tdSql.query(sql)
    tdSql.checkRows(len(expected_rows))
    want_cols = expected_cols if expected_cols is not None else (
        len(expected_rows[0]) if expected_rows else None
    )
    if want_cols is not None:
        assert tdSql.queryCols == want_cols, (
            f"{sql}: expected {want_cols} columns, got {tdSql.queryCols}"
        )
    for row_index, expected in enumerate(expected_rows):
        for col_index, value in enumerate(expected):
            tdSql.checkData(row_index, col_index, value)


def _aggregate(values):
    nonnull = [value for value in values if value is not None]
    if not nonnull:
        return [len(values), None, None, None, None]
    return [
        len(values),
        max(nonnull),
        min(nonnull),
        sum(nonnull),
        float(sum(nonnull)) / len(nonnull),
    ]


def _nonnull_stats(values):
    """count / sum / avg / max / min with SQL NULL semantics."""
    nonnull = [value for value in values if value is not None]
    if not nonnull:
        return [0, None, None, None, None]
    total = sum(nonnull)
    return [len(nonnull), total, float(total) / len(nonnull), max(nonnull), min(nonnull)]


def _none_first_key(value):
    return (0, 0) if value is None else (1, value)


def _nonnull(values):
    return [value for value in values if value is not None]


def _sum_or_none(values):
    nonnull = _nonnull(values)
    return sum(nonnull) if nonnull else None


def _avg_or_none(values):
    nonnull = _nonnull(values)
    return float(sum(nonnull)) / len(nonnull) if nonnull else None


def syntax_variants(shape):
    cols = shape["columns"]
    first = cols[0]
    variants = [
        ("default", None, "", False),
        ("explicit_default", "FALSE, '*:PREV:'", "*:PREV:", False),
        ("star_suffix", "'*:PREV:_a'", "*:PREV:_a", False),
        ("star_raw", "TRUE, '*:PREV:_a'", "*:PREV:_a", True),
        ("single", f"'{first}'", first, False),
        ("colon_default", f"'{first}:'", first, False),
        ("double_colon_default", f"'{first}::'", first, False),
        ("suffix", f"'{first}::_ff'", f"{first}::_ff", False),
        ("method", f"'{first}:PREV:'", f"{first}:PREV:", False),
        ("method_lower", f"'{first}:prev:_lc'", f"{first}:prev:_lc", False),
        ("double_star", "'*:PREV:_a;*::_b'", "*:PREV:_a;*::_b", False),
        ("raw_single", f"TRUE, '{first}:PREV:_a'", f"{first}:PREV:_a", True),
    ]
    if len(cols) >= 2:
        second = cols[1]
        variants.append(("multi", f"'{first}:PREV:_a;{second}::_b'", f"{first}:PREV:_a;{second}::_b", False))
        variants.append(("star_plus", f"'*:PREV:_a;{first}:PREV:_b'", f"*:PREV:_a;{first}:PREV:_b", False))
    return variants


class TestTimeAlign:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute(f"create database {DB}")
        tdSql.execute(f"use {DB}")
        TestTimeAlign.prepare_sources()
        TestTimeAlign.prepare_tables()
        TestTimeAlign.prepare_source_variants()

    def teardown_class(cls):
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute("drop database if exists ta_align_x")

    @staticmethod
    def prepare_sources():
        for table, schema in SOURCE_SCHEMAS.items():
            columns = ", ".join(f"{name} {typ}" for name, typ in schema)
            tdSql.execute(f"create table {table} (ts timestamp, {columns})")

        stable_columns = ", ".join(f"{name} {typ}" for name, typ in STABLE_SCHEMA)
        tdSql.execute(f"create stable {STABLE_NAME} (ts timestamp, {stable_columns}) tags ({STABLE_TAG})")
        for child, site in CHILDREN.items():
            tdSql.execute(f"create table {child} using {STABLE_NAME} tags({site})")

        for table, rows in SOURCE_ROWS.items():
            if not rows:
                continue
            schema = SOURCE_SCHEMAS.get(table, STABLE_SCHEMA)
            columns = [name for name, _ in schema]
            values = []
            for offset in sorted(rows):
                cells = ", ".join(_sql_value(rows[offset].get(col)) for col in columns)
                values.append(f"({ts(offset)}, {cells})")
            tdSql.execute(f"insert into {table} values " + " ".join(values))

    @staticmethod
    def prepare_tables():
        # virtual super tables first, then their child vtables
        for shape in SHAPES.values():
            if shape["kind"] != "vstable":
                continue
            columns = ", ".join(
                f"{vcol} {_source_type(src, srccol)}"
                for vcol, src, srccol in shape["children"][0]["columns"]
            )
            tdSql.execute(
                f"create stable {shape['name']} (ts timestamp, {columns}) tags ({STABLE_TAG}) virtual 1"
            )

        for shape in SHAPES.values():
            if shape["kind"] != "vtable" or "parent" not in shape:
                continue
            child = shape["children"][0]
            refs = ", ".join(f"{src}.{srccol}" for _, src, srccol in child["columns"])
            tdSql.execute(
                f"create vtable {shape['name']} ({refs}) using {shape['parent']} tags({child['site']})"
            )

        for shape in SHAPES.values():
            if shape["kind"] != "vtable" or "parent" in shape:
                continue
            child = shape["children"][0]
            columns = ", ".join(
                f"{vcol} {_source_type(src, srccol)} from {src}.{srccol}"
                for vcol, src, srccol in child["columns"]
            )
            tdSql.execute(
                f"create vtable {shape['name']} (ts timestamp, {columns}){shape.get('ddl_suffix', '')}"
            )

    @staticmethod
    def prepare_source_variants():
        # side table used by the relational matrix (join/subquery)
        tdSql.execute("create table t_side (ts timestamp, label int)")
        tdSql.execute(f"insert into t_side values ({ts(0)}, 100) ({ts(2000)}, 200) ({ts(5000)}, 500)")

        # nested virtual table: a vtable column referencing another vtable column
        tdSql.execute(
            "create vtable vt_nested (ts timestamp, voltage double from vt_gaps.voltage)")

        # wider-than-declared source column: the aligned value must keep the full width
        tdSql.execute("create table t_wide (ts timestamp, b binary(64))")
        tdSql.execute(
            f"insert into t_wide values ({ts(0)}, '0123456789abcdefghijklmnopqrstuvwxyz') "
            f"({ts(1000)}, NULL) "
            f"({ts(2000)}, 'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ')")
        tdSql.execute("create vtable vt_wide (ts timestamp, b binary(8) from t_wide.b)")

        # VARCHAR source wider than the declared column: a 1200-byte value cannot
        # fit the varchar(4) align output and must fail with a clean 0x070B error.
        tdSql.execute("create table t_vchar_n (ts timestamp, v varchar(2048))")
        tdSql.execute(
            f"insert into t_vchar_n values ({ts(0)}, 'aa') ({ts(1000)}, NULL) "
            f"({ts(2000)}, 'bb') ({ts(3000)}, 'cc')")
        tdSql.execute("create vtable vt_vchar_n (ts timestamp, v varchar(4) from t_vchar_n.v)")

        tdSql.execute("create table t_vchar_w (ts timestamp, v varchar(2048))")
        tdSql.execute(f"insert into t_vchar_w values ({ts(0)}, 'aa') ({ts(1000)}, '{WIDE_VC}')")
        tdSql.execute("create vtable vt_vchar_w (ts timestamp, v varchar(4) from t_vchar_w.v)")
        tdSql.execute("create stable vst_vchar_w (ts timestamp, v varchar(4)) tags (loc varchar(8)) virtual 1")
        tdSql.execute("create vtable vc_vchar_w (v from t_vchar_w.v) using vst_vchar_w tags ('l1')")

        # cross-database + cross-precision source: ns source -> ms vtable
        tdSql.execute("drop database if exists ta_align_x")
        tdSql.execute("create database ta_align_x precision 'ns'")
        tdSql.execute("create table ta_align_x.t_x (ts timestamp, v double)")
        tdSql.execute(
            "insert into ta_align_x.t_x values "
            "(1767225600123456789, 10) (1767225601123456789, NULL) (1767225602123456789, 30)")
        tdSql.execute("create vtable vt_x (ts timestamp, v double from ta_align_x.t_x.v)")

        # Negative-case fixtures for final output-name conflicts (FS 4.4.3).
        # Two sources that can produce the same final name: a::_b and a_b.
        tdSql.execute("create table t_coll (ts timestamp, a int, a_b int)")
        tdSql.execute(f"insert into t_coll values ({ts(0)}, 1, 2)")
        tdSql.execute(
            "create vtable vt_coll (ts timestamp, a int from t_coll.a, "
            "a_b int from t_coll.a_b)")
        # The source column is named t, so the suffix 's' produces the
        # primary-timestamp name 'ts'.
        tdSql.execute("create vtable vt_pkname (ts timestamp, t int from t_gaps.status)")
        # Owned tag named voltage: an aligned column may not reuse the name,
        # even when raw columns are not part of the output.
        tdSql.execute(
            "create vtable vt_tagconflict (ts timestamp, v double from t_gaps.voltage) "
            "tags (voltage int = 1)")

        # A view over a virtual table is a phase-1 unsupported input type;
        # a view over TIME_ALIGN is allowed (FS 4.8.3.3) and is checked as a
        # positive case.
        tdSql.execute("create view v_vt_gaps as select * from vt_gaps")

        # Quoted identifiers whose names contain the definition separators.
        tdSql.execute("create table t_quote (ts timestamp, `a:b` int, `c;d` int)")
        tdSql.execute(f"insert into t_quote values ({ts(0)}, 1, 2) ({ts(1000)}, 3, 4)")
        tdSql.execute(
            "create vtable vt_quote (ts timestamp, `a:b` int from t_quote.`a:b`, "
            "`c;d` int from t_quote.`c;d`)")
        # Doubled backticks escape a backtick inside a quoted identifier.
        tdSql.execute("create table t_bt (ts timestamp, `a``b` int)")
        tdSql.execute(f"insert into t_bt values ({ts(0)}, 7) ({ts(1000)}, 8)")
        tdSql.execute("create vtable vt_bt (ts timestamp, `a``b` int from t_bt.`a``b`)")

    def test_filter_state_matrix(self):
        """Filters must not truncate the PREV input.

        A ts range or a raw-column predicate filters output rows only; rows
        outside the filter still feed PREV.  The model computes the full-frame
        aligned values first and filters afterwards, so the first surviving
        row must carry a value filled from a row that is not in the result.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, matrix, filter, ci
        """
        for shape in SHAPES.values():
            cols = shape["columns"]
            first = cols[0]
            a0 = f"{first}_a"
            tagged = _shape_has_site(shape)
            site_sel = "site, " if tagged else ""
            order = "site, ts" if tagged else "ts"
            frame = build_frame(shape, {first}, False)
            timestamps = sorted({row["ts"] for row in frame})
            cutoff = timestamps[1] if len(timestamps) > 1 else (
                timestamps[0] if timestamps else BASE_TS + 1000)

            # 1. ts range: rows before the range still provide PREV.
            expected = [
                ([row["site"], row["ts"], row["align"][first]] if tagged
                 else [row["ts"], row["align"][first]])
                for row in frame if row["ts"] >= cutoff
            ]
            _assert_query(
                f"select {site_sel}ts, {a0} from "
                f"table(time_align(table {shape['name']}, '{first}:PREV:_a')) "
                f"where ts >= {cutoff} order by {order}",
                expected,
            )

            # 2. raw predicate with output_raw=TRUE: filtered raw rows still
            # feed the aligned values of the surviving rows.
            frame_raw = build_frame(shape, {first}, True)
            raw_values = sorted({row["raw"][first] for row in frame_raw if row["raw"][first] is not None})
            threshold = raw_values[len(raw_values) // 2] if raw_values else 0
            expected = [
                ([row["site"], row["ts"], row["raw"][first], row["align"][first]] if tagged
                 else [row["ts"], row["raw"][first], row["align"][first]])
                for row in frame_raw
                if row["raw"][first] is not None and row["raw"][first] >= threshold
            ]
            _assert_query(
                f"select {site_sel}ts, {first}, {a0} from "
                f"table(time_align(table {shape['name']}, TRUE, '{first}:PREV:_a')) "
                f"where {first} is not null and {first} >= {threshold} order by {order}",
                expected,
            )

            # 3. ts range combined with an aligned-column predicate.
            aligned_values = sorted({row["align"][first] for row in frame if row["align"][first] is not None})
            value_threshold = aligned_values[len(aligned_values) // 2] if aligned_values else 0
            expected = [
                ([row["site"], row["ts"], row["align"][first]] if tagged
                 else [row["ts"], row["align"][first]])
                for row in frame
                if row["ts"] >= cutoff
                and (row["align"][first] is not None and row["align"][first] >= value_threshold)
            ]
            _assert_query(
                f"select {site_sel}ts, {a0} from "
                f"table(time_align(table {shape['name']}, '{first}:PREV:_a')) "
                f"where ts >= {cutoff} and {a0} >= {value_threshold} order by {order}",
                expected,
            )

    def test_group_having_distinct_matrix(self):
        """GROUP BY / HAVING / DISTINCT on raw and aligned columns.

        - group by the aligned column, aggregating another aligned column,
          with HAVING filtering whole groups (NULL is a group and sorts first)
        - group by a raw column while aggregating aligned columns in the same
          statement (output_raw=TRUE)
        - DISTINCT over a raw+aligned column pair

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, matrix, group, ci
        """
        for shape in SHAPES.values():
            cols = shape["columns"]
            first = cols[0]
            second = cols[1] if len(cols) > 1 else first
            a0 = f"{first}_a"
            a1 = f"{second}_a"
            name = shape["name"]

            # 1. GROUP BY aligned column + HAVING count(*) >= 2.
            frame = build_frame(shape, set(cols), False)
            groups = {}
            for row in frame:
                groups.setdefault(row["align"][first], []).append(row)
            expected = []
            for key in sorted(groups, key=_none_first_key):
                rows = groups[key]
                if len(rows) < 2:
                    continue
                values = [row["align"][second] for row in rows]
                expected.append([key, len(rows), _sum_or_none(values), _avg_or_none(values)])
            _assert_query(
                f"select {a0}, count(*), sum({a1}), avg({a1}) from "
                f"table(time_align(table {name}, '*:PREV:_a')) "
                f"group by {a0} having count(*) >= 2 order by {a0}",
                expected,
            )

            # 2. GROUP BY a raw column while aggregating aligned columns.
            if "status" in cols:
                frame_raw = build_frame(shape, set(cols), True)
                groups = {}
                for row in frame_raw:
                    groups.setdefault(row["raw"]["status"], []).append(row)
                expected = []
                for key in sorted(groups, key=_none_first_key):
                    rows = groups[key]
                    expected.append([key, len(rows), _sum_or_none([row["align"][first] for row in rows])])
                _assert_query(
                    f"select status, count(*), sum({a0}) from "
                    f"table(time_align(table {name}, TRUE, '*::_a')) "
                    f"group by status order by status",
                    expected,
                )

            # 3. DISTINCT over the (raw, aligned) pair.
            frame_raw = build_frame(shape, {first}, True)
            pairs = {(row["raw"][first], row["align"][first]) for row in frame_raw}
            expected = [
                [pair[0], pair[1]]
                for pair in sorted(pairs, key=lambda pair: (_none_first_key(pair[0]), _none_first_key(pair[1])))
            ]
            _assert_query(
                f"select distinct {first}, {a0} from "
                f"table(time_align(table {name}, TRUE, '{first}:PREV:_a')) "
                f"order by {first}, {a0}",
                expected,
            )

    def test_window_variant_matrix(self):
        """SESSION / COUNT / STATE / offset-INTERVAL / sliding / FILL(PREV).

        Window functions must see the aligned row set exactly like a plain
        table: row counts and aligned-value aggregates per window are compared
        with the model.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, matrix, window, ci
        """
        for shape in SHAPES.values():
            cols = shape["columns"]
            first = cols[0]
            a0 = f"{first}_a"
            name = shape["name"]
            tagged = _shape_has_site(shape)
            frame = build_frame(shape, {first}, False)
            if not frame:
                continue
            frame_by_ts = sorted(frame, key=lambda row: row["ts"])

            # 1. SESSION(ts, 500a): split when the gap between rows exceeds 500ms.
            expected = []
            group = []
            previous_ts = None
            for row in frame_by_ts:
                if group and previous_ts is not None and row["ts"] - previous_ts > 500:
                    expected.append([group[0]["ts"], len(group),
                                     _sum_or_none([item["align"][first] for item in group])])
                    group = []
                group.append(row)
                previous_ts = row["ts"]
            if group:
                expected.append([group[0]["ts"], len(group),
                                 _sum_or_none([item["align"][first] for item in group])])
            _assert_query(
                f"select _wstart, count(*), sum({a0}) from "
                f"table(time_align(table {name}, '{first}:PREV:_a')) "
                f"session(ts, 500a) order by _wstart",
                expected,
            )

            # 2. COUNT_WINDOW(3): fixed row-count windows (unique timestamps only).
            if not tagged and len({row["ts"] for row in frame}) == len(frame):
                expected = []
                for start in range(0, len(frame_by_ts), 3):
                    chunk = frame_by_ts[start:start + 3]
                    expected.append([chunk[0]["ts"], len(chunk),
                                     _sum_or_none([item["align"][first] for item in chunk])])
                _assert_query(
                    f"select _wstart, count(*), sum({a0}) from "
                    f"table(time_align(table {name}, '{first}:PREV:_a')) "
                    f"count_window(3) order by _wstart",
                    expected,
                )

            # 3. STATE_WINDOW over an integer aligned column.
            if "status" in cols and not tagged:
                status_frame = build_frame(shape, {"status"}, False)
                expected = []
                state = None
                group = []
                for row in status_frame:
                    value = row["align"]["status"]
                    if group and value != state:
                        expected.append([group[0]["ts"], group[-1]["ts"], len(group), state])
                        group = []
                    state = value
                    group.append(row)
                if group:
                    expected.append([group[0]["ts"], group[-1]["ts"], len(group), state])
                _assert_query(
                    f"select _wstart, _wend, count(*), first(status_a) from "
                    f"table(time_align(table {name}, 'status:PREV:_a')) "
                    f"state_window(status_a) order by _wstart",
                    expected,
                )

            # 4. STATE_WINDOW on the raw column (output_raw=TRUE): NULL rows
            # do not form states; consecutive equal raw values do.
            if "status" in cols and not tagged:
                raw_frame = build_frame(shape, set(cols), True)
                rows = [row for row in raw_frame if row["raw"]["status"] is not None]
                expected = []
                group = []
                state = None
                for row in rows:
                    value = row["raw"]["status"]
                    if group and value != state:
                        expected.append([group[0]["ts"], group[-1]["ts"], len(group), state])
                        group = []
                    state = value
                    group.append(row)
                if group:
                    expected.append([group[0]["ts"], group[-1]["ts"], len(group), state])
                _assert_query(
                    f"select _wstart, _wend, count(*), first(status) from "
                    f"table(time_align(table {name}, TRUE, '*::_a')) "
                    f"state_window(status) order by _wstart",
                    expected,
                )

            # 5. INTERVAL(2s, 1s): fixed 1s offset grid.
            groups = {}
            for row in frame:
                window = (row["ts"] - BASE_TS - 1000) // 2000
                groups.setdefault(window, []).append(row)
            expected = []
            for window in sorted(groups):
                rows = groups[window]
                expected.append([BASE_TS + 1000 + window * 2000, len(rows),
                                 _sum_or_none([item["align"][first] for item in rows])])
            _assert_query(
                f"select _wstart, count(*), sum({a0}) from "
                f"table(time_align(table {name}, '{first}:PREV:_a')) "
                f"interval(2s, 1s) order by _wstart",
                expected,
            )

            # 6. INTERVAL(2s) SLIDING(1s): overlapping 2s windows every 1s.
            min_ts = frame_by_ts[0]["ts"]
            max_ts = frame_by_ts[-1]["ts"]
            start = ((min_ts - 2000) // 1000 + 1) * 1000
            expected = []
            while start <= max_ts:
                rows = [row for row in frame if start <= row["ts"] < start + 2000]
                if rows:
                    expected.append([start, len(rows),
                                     _sum_or_none([item["align"][first] for item in rows])])
                start += 1000
            _assert_query(
                f"select _wstart, count(*), sum({a0}) from "
                f"table(time_align(table {name}, '{first}:PREV:_a')) "
                f"interval(2s) sliding(1s) order by _wstart",
                expected,
            )

            # 7. EVENT_WINDOW start/end conditions on aligned values.  Rows
            # with duplicate timestamps across vstable children have no
            # deterministic order, so exact window boundaries are asserted only
            # when the aligned row set has unique timestamps.
            first_values = _nonnull([row["align"][first] for row in frame_by_ts])
            if first_values and len({row["ts"] for row in frame}) == len(frame):
                ordered = sorted(first_values)
                start_threshold = ordered[len(ordered) // 3]
                end_threshold = ordered[(2 * len(ordered)) // 3]
                rows = frame_by_ts
                expected = []
                index = 0
                while index < len(rows):
                    while index < len(rows) and not (
                            rows[index]["align"][first] is not None
                            and rows[index]["align"][first] >= start_threshold):
                        index += 1
                    if index >= len(rows):
                        break
                    start_ts = rows[index]["ts"]
                    end_index = index
                    while end_index < len(rows) and not (
                            rows[end_index]["align"][first] is not None
                            and rows[end_index]["align"][first] > end_threshold):
                        end_index += 1
                    if end_index >= len(rows):
                        break  # a start without a matching end emits no window
                    end_ts = rows[end_index]["ts"]
                    window_rows = [row for row in rows if start_ts <= row["ts"] <= end_ts]
                    expected.append([start_ts, end_ts, len(window_rows),
                                     _sum_or_none([row["align"][first] for row in window_rows])])
                    index = end_index + 1
                    while index < len(rows) and rows[index]["ts"] <= end_ts:
                        index += 1
                _assert_query(
                    f"select _wstart, _wend, count(*), sum({a0}) from "
                    f"table(time_align(table {name}, '{first}:PREV:_a')) "
                    f"event_window start with {a0} >= {start_threshold} "
                    f"end with {a0} > {end_threshold} order by _wstart",
                    expected,
                )

            # 8. INTERVAL(2s) FILL(PREV): empty windows repeat the previous
            # window's aggregate values.
            range_start = min_ts
            range_end = max_ts
            groups = {}
            for row in frame:
                window = (row["ts"] - BASE_TS) // 2000
                groups.setdefault(window, []).append(row)
            first_window = (range_start - BASE_TS) // 2000
            last_window = (range_end - BASE_TS) // 2000
            expected = []
            previous = None
            for window in range(first_window, last_window + 1):
                rows = groups.get(window, [])
                if rows:
                    values = [item["align"][first] for item in rows]
                    previous = [len(rows), _sum_or_none(values), _avg_or_none(values)]
                expected.append([BASE_TS + window * 2000] + previous)
            _assert_query(
                f"select _wstart, count(*), sum({a0}), avg({a0}) from "
                f"table(time_align(table {name}, '{first}:PREV:_a')) "
                f"where ts >= {range_start} and ts <= {range_end} "
                f"interval(2s) fill(prev) order by _wstart",
                expected,
            )

    def test_order_partition_raw_aligned_matrix(self):
        """ORDER BY and PARTITION BY on raw and aligned columns.

        - ORDER BY the aligned column ASC/DESC with LIMIT
        - ORDER BY the raw column (output_raw=TRUE)
        - PARTITION BY the aligned column + INTERVAL
        - PARTITION BY the raw column + INTERVAL (NULL is its own partition)
        - PARTITION BY a default-named aligned column

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, matrix, order, partition, ci
        """
        for shape in SHAPES.values():
            cols = shape["columns"]
            first = cols[0]
            a0 = f"{first}_a"
            name = shape["name"]
            tagged = _shape_has_site(shape)
            site_sel = "site, " if tagged else ""
            order = "site, ts" if tagged else "ts"
            frame = build_frame(shape, {first}, False)
            if not frame:
                continue

            # 1. ORDER BY aligned column ASC + ts, LIMIT 5.
            selected = sorted(frame, key=lambda row: (_none_first_key(row["align"][first]), row["ts"]))[:5]
            expected = [
                ([row["site"], row["ts"], row["align"][first]] if tagged
                 else [row["ts"], row["align"][first]])
                for row in selected
            ]
            _assert_query(
                f"select {site_sel}ts, {a0} from "
                f"table(time_align(table {name}, '{first}:PREV:_a')) "
                f"order by {a0}, ts limit 5",
                expected,
            )

            # 2. ORDER BY aligned column DESC + ts, LIMIT 5 (NULLs excluded).
            selected = sorted(
                [row for row in frame if row["align"][first] is not None],
                key=lambda row: (-row["align"][first], row["ts"]),
            )[:5]
            expected = [
                ([row["site"], row["ts"], row["align"][first]] if tagged
                 else [row["ts"], row["align"][first]])
                for row in selected
            ]
            _assert_query(
                f"select {site_sel}ts, {a0} from "
                f"table(time_align(table {name}, '{first}:PREV:_a')) "
                f"where {a0} is not null order by {a0} desc, ts limit 5",
                expected,
            )

            # 3. ORDER BY the raw column with output_raw=TRUE.
            frame_raw = build_frame(shape, {first}, True)
            selected = sorted(
                [row for row in frame_raw if row["raw"][first] is not None],
                key=lambda row: (row["raw"][first], row["ts"]),
            )[:5]
            expected = [
                ([row["site"], row["ts"], row["raw"][first], row["align"][first]] if tagged
                 else [row["ts"], row["raw"][first], row["align"][first]])
                for row in selected
            ]
            _assert_query(
                f"select {site_sel}ts, {first}, {a0} from "
                f"table(time_align(table {name}, TRUE, '{first}:PREV:_a')) "
                f"where {first} is not null order by {first}, ts limit 5",
                expected,
            )

            if "status" not in cols or tagged:
                continue

            # 4. PARTITION BY aligned column + INTERVAL.
            status_frame = build_frame(shape, set(cols), False)
            groups = {}
            for row in status_frame:
                groups.setdefault(row["align"]["status"], []).append(row)
            expected = []
            for status in sorted(groups, key=_none_first_key):
                windows = {}
                for row in groups[status]:
                    window = (row["ts"] - BASE_TS) // 2000
                    windows.setdefault(window, []).append(row)
                for window in sorted(windows):
                    window_rows = windows[window]
                    expected.append([
                        status,
                        BASE_TS + window * 2000,
                        len(window_rows),
                        _sum_or_none([row["align"][first] for row in window_rows]),
                    ])
            _assert_query(
                f"select status_a, _wstart, count(*), sum({a0}) from "
                f"table(time_align(table {name}, '*:PREV:_a')) "
                f"partition by status_a interval(2s) order by status_a, _wstart",
                expected,
            )

            # 5. PARTITION BY raw column + INTERVAL (NULL is its own partition).
            frame_all = build_frame(shape, set(cols), True)
            groups = {}
            for row in frame_all:
                groups.setdefault(row["raw"]["status"], []).append(row)
            expected = []
            for status in sorted(groups, key=_none_first_key):
                windows = {}
                for row in groups[status]:
                    window = (row["ts"] - BASE_TS) // 2000
                    windows.setdefault(window, []).append(row)
                for window in sorted(windows):
                    window_rows = windows[window]
                    expected.append([
                        status,
                        BASE_TS + window * 2000,
                        len(window_rows),
                        _sum_or_none([row["align"][first] for row in window_rows]),
                    ])
            _assert_query(
                f"select status, _wstart, count(*), sum({a0}) from "
                f"table(time_align(table {name}, TRUE, '*::_a')) "
                f"partition by status interval(2s) order by status, _wstart",
                expected,
            )

            # 6. PARTITION BY a default-named aligned column: default arguments
            # align every column, so the default name resolves to the aligned
            # values (no raw leakage).
            aligned_groups = {}
            for row in status_frame:
                aligned_groups.setdefault(row["align"]["status"], []).append(row)
            expected = [
                [status, len(rows), _sum_or_none([row["align"][first] for row in rows])]
                for status, rows in sorted(aligned_groups.items(), key=lambda item: _none_first_key(item[0]))
            ]
            _assert_query(
                f"select status, count(*), sum({first}) from "
                f"table(time_align(table {name})) "
                f"partition by status order by status",
                expected,
            )

    def test_special_aggregate_matrix(self):
        """FIRST/LAST/COUNT DISTINCT/TOP/BOTTOM/PERCENTILE on aligned columns.

        Aggregates are checked against a Python model of each function; FIRST
        and LAST ignore leading NULLs, while PERCENTILE and TOP/BOTTOM use the
        engine's interpolation and ordering rules.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, matrix, aggregate, ci
        """
        for shape in SHAPES.values():
            if shape["kind"] != "vtable":
                continue  # percentile/TOP on a vstable is rejected by the engine
            first = shape["columns"][0]
            a0 = f"{first}_a"
            name = shape["name"]
            frame = build_frame(shape, {first}, False)
            values = [row["align"][first] for row in frame]
            nonnull = _nonnull(values)

            # FIRST/LAST ignore leading NULLs; COUNT(DISTINCT) ignores NULLs.
            expected = [[
                len(values),
                len(set(nonnull)),
                nonnull[0] if nonnull else None,
                nonnull[-1] if nonnull else None,
            ]]
            _assert_query(
                f"select count(*), count(distinct {a0}), first({a0}), last({a0}) from "
                f"table(time_align(table {name}, '{first}:PREV:_a'))",
                expected,
            )

            # TOP returns the largest values ascending, BOTTOM the smallest
            # values descending (both ignore NULLs).
            top_n = min(2, len(nonnull))
            if top_n:
                ascending = sorted(nonnull)
                top_values = ascending[len(ascending) - top_n:]
                bottom_values = ascending[:top_n][::-1]
                expected = [[top_values[i], bottom_values[i]] for i in range(top_n)]
                _assert_query(
                    f"select top({a0}, 2), bottom({a0}, 2) from "
                    f"table(time_align(table {name}, '{first}:PREV:_a'))",
                    expected,
                )

            # PERCENTILE with linear interpolation between the two nearest ranks.
            def percentile_value(p):
                if not nonnull:
                    return None
                ordered = sorted(nonnull)
                rank = (len(ordered) - 1) * p / 100.0
                low = int(rank)
                high = min(low + 1, len(ordered) - 1)
                frac = rank - low
                if frac == 0:
                    return ordered[low]
                return ordered[low] + (ordered[high] - ordered[low]) * frac

            if nonnull:
                expected = [[percentile_value(0), percentile_value(25),
                             percentile_value(50), percentile_value(100)]]
            else:
                expected = []  # PERCENTILE over no rows returns no row
            _assert_query(
                f"select percentile({a0}, 0), percentile({a0}, 25), "
                f"percentile({a0}, 50), percentile({a0}, 100) from "
                f"table(time_align(table {name}, '{first}:PREV:_a'))",
                expected,
            )

    def test_relational_matrix(self):
        """JOIN / derived table / IN subquery / UNION ALL over TIME_ALIGN.

        The aligned rows are joined, filtered through IN subqueries and
        combined with UNION ALL; every result is compared against the model.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, matrix, relation, ci
        """
        shape = SHAPES["vt_gaps"]
        frame = build_frame(shape, {"voltage"}, False)
        frame_rows = [(row["ts"], row["align"]["voltage"]) for row in frame]
        side = {ts(0): 100, ts(2000): 200, ts(5000): 500}

        # INNER JOIN on ts.
        expected = [[row_ts, value, side[row_ts]] for row_ts, value in frame_rows if row_ts in side]
        _assert_query(
            "select x.ts, x.voltage_a, s.label from "
            "table(time_align(table vt_gaps, 'voltage:PREV:_a')) x "
            "join t_side s on x.ts = s.ts order by x.ts",
            expected,
        )

        # LEFT JOIN on ts.
        expected = [[row_ts, value, side.get(row_ts)] for row_ts, value in frame_rows]
        _assert_query(
            "select x.ts, x.voltage_a, s.label from "
            "table(time_align(table vt_gaps, 'voltage:PREV:_a')) x "
            "left join t_side s on x.ts = s.ts order by x.ts",
            expected,
        )

        # Derived table with a filter on an aligned column.
        expected = [[row_ts, value] for row_ts, value in frame_rows if value is not None and value >= 3]
        _assert_query(
            "select * from (select ts, voltage_a from "
            "table(time_align(table vt_gaps, 'voltage:PREV:_a')) "
            "where voltage_a >= 3) t order by ts",
            expected,
        )

        # IN subquery built from the same aligned column.
        expected = [[row_ts, value] for row_ts, value in frame_rows if value is not None and value > 3]
        _assert_query(
            "select ts, voltage_a from table(time_align(table vt_gaps, "
            "'voltage:PREV:_a;current:PREV:_a')) "
            "where voltage_a in (select voltage_a from "
            "table(time_align(table vt_gaps, 'voltage:PREV:_a')) where voltage_a > 3) "
            "order by ts",
            expected,
        )

        # UNION ALL of two different aligned definitions.
        current_frame = build_frame(shape, {"current"}, False)
        current_rows = [(row["ts"], row["align"]["current"]) for row in current_frame]
        merged = sorted(frame_rows + current_rows, key=lambda item: (item[0], item[1]))
        expected = [[row_ts, value] for row_ts, value in merged]
        _assert_query(
            "(select ts, voltage_a from table(time_align(table vt_gaps, "
            "'voltage:PREV:_a'))) union all "
            "(select ts, current_a from table(time_align(table vt_gaps, "
            "'current:PREV:_a'))) order by ts, voltage_a",
            expected,
        )

    def test_source_variant_matrix(self):
        """Cross-db / ns precision / nested vtable / wide source column.

        Cross-database naming, nanosecond-to-millisecond conversion, nested
        virtual tables, a view over the table function and variable-length
        values wider than the declared virtual column are all model-checked.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, matrix, source, ci
        """
        # 1. Cross-database qualified table function.
        frame = build_frame(SHAPES["vt_gaps"], {"voltage"}, False)
        expected = [[row["ts"], row["align"]["voltage"]] for row in frame]
        tdSql.execute("use ta_align_x")
        try:
            _assert_query(
                f"select ts, voltage_a from table(time_align(table {DB}.vt_gaps, "
                f"'voltage:PREV:_a')) order by ts",
                expected,
            )
        finally:
            tdSql.execute(f"use {DB}")

        # 2. ns source -> ms vtable: timestamps are converted, values aligned.
        expected = [
            [BASE_TS + 123, 10],
            [BASE_TS + 1123, 10],
            [BASE_TS + 2123, 30],
        ]
        _assert_query(
            "select ts, v_a from table(time_align(table vt_x, 'v:PREV:_a')) order by ts",
            expected,
        )
        expected = [
            [BASE_TS + 0, 1, 10],
            [BASE_TS + 1000, 1, 10],
            [BASE_TS + 2000, 1, 30],
        ]
        _assert_query(
            "select _wstart, count(*), sum(v_a) from "
            "table(time_align(table vt_x, 'v:PREV:_a')) interval(1s) order by _wstart",
            expected,
        )

        # 3. Nested virtual table (vtable column references a vtable column).
        expected = [[row["ts"], row["align"]["voltage"]] for row in frame]
        _assert_query(
            "select ts, voltage_a from table(time_align(table vt_nested, "
            "'voltage:PREV:_a')) order by ts",
            expected,
        )

        # 4. Source value wider than the declared virtual column: the aligned
        # value must keep the full source width (not the declared binary(8)).
        _assert_query(
            "select ts, length(b), length(b_a) from "
            "table(time_align(table vt_wide, TRUE, 'b:PREV:_a')) order by ts",
            [[ts(0), 36, 36], [ts(1000), None, 36], [ts(2000), 36, 36]],
        )

        # 5. A view over TIME_ALIGN is allowed (FS 4.8.3.3) and keeps the
        # aligned values; phase 2 additionally allows using that view as a
        # TIME_ALIGN input (see test_time_align_phase2.py).
        tdSql.execute(
            "create view v_align as select * from table(time_align(table vt_gaps))")
        expected = [
            [row["ts"]] + [row["align"][col] for col in COLS3]
            for row in build_frame(SHAPES["vt_gaps"], set(COLS3), False)
        ]
        _assert_query("select * from v_align order by ts", expected, 4)

    def test_wide_varchar_source(self):
        """VARCHAR source wider than the declared column: align in full or fail cleanly.

        A value that fits varchar(4) aligns normally.  On a flat virtual table
        the planner widens the scan column to the source width, so a 1200-byte
        value is returned in full; on a virtual super table the declared
        varchar(4) is kept and the wide value fails with 0x070B.

        1. Narrow-only source aligns every value and carries NULL gaps
        2. Flat virtual table returns the full 1200-byte value
        3. Virtual super table over the wide source fails with 0x070B

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, width, negative, ci

        Jira: None

        History:
            - 2026-11-18 Created
        """
        tdSql.query(
            "select ts, v_a from table(time_align(table vt_vchar_n, 'v:PREV:_a')) order by ts")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, ts(0))
        tdSql.checkData(0, 1, "aa")
        tdSql.checkData(1, 0, ts(1000))
        tdSql.checkData(1, 1, "aa")
        tdSql.checkData(2, 0, ts(2000))
        tdSql.checkData(2, 1, "bb")
        tdSql.checkData(3, 0, ts(3000))
        tdSql.checkData(3, 1, "cc")

        # flat virtual table: the planner widens the scan column to the source
        # width, so the full 1200-byte value is returned (not an error).
        tdSql.query(
            "select v_a from table(time_align(table vt_vchar_w, 'v:PREV:_a')) order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "aa")
        tdSql.checkData(1, 0, WIDE_VC)

        # virtual super table: children may reference differently-widthed sources,
        # so the declared varchar(4) is kept and the wide value fails with 0x070B.
        tdSql.error("select v_a from table(time_align(table vst_vchar_w, 'v:PREV:_a'))", 0x8000070B)

    def test_syntax_matrix(self):
        """Every syntax variant on every table shape/distribution.

        Each generated query is `SELECT *` over the table function, ordered by
        ts (and site for vstable), and is compared value-by-value against the
        reference model: raw columns, aligned columns, tag columns, column
        count and row count.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, matrix, syntax, ci
        """
        for shape in SHAPES.values():
            order = "site, ts" if _shape_has_site(shape) else "ts"
            for variant, args, def_text, output_raw in syntax_variants(shape):
                entries = parse_definition(def_text, shape["columns"])
                expected = natural_rows(shape, entries, output_raw)
                expected_cols = (
                    1
                    + (len(shape["columns"]) if output_raw else 0)
                    + len(entries)
                    + (1 if output_raw and shape["kind"] == "vstable" else 0)
                )
                args_sql = "" if args is None else f", {args}"
                sql = (
                    f"select * from table(time_align(table {shape['name']}{args_sql})) "
                    f"order by {order}"
                )
                tdLog.info(f"[{shape['name']}/{variant}]")
                _assert_query(sql, expected, expected_cols)

    def test_query_matrix(self):
        """Common query shapes on every table shape/distribution.

        Runs projection + aligned-column WHERE, ORDER BY DESC + LIMIT/OFFSET,
        aggregates (per tag for tagged tables), INTERVAL windows (PARTITION BY
        tag for tagged tables), expressions over two aligned columns and
        output_raw raw-column filtering.  All results are model-checked.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, matrix, query, ci
        """
        for shape in SHAPES.values():
            cols = shape["columns"]
            first = cols[0]
            second = cols[1] if len(cols) > 1 else None
            first_a = f"{first}_a"
            second_a = f"{second}_a" if second else None
            tagged = _shape_has_site(shape)
            site_sel = "site, " if tagged else ""
            order = "site, ts" if tagged else "ts"
            defs1 = f"'{first}:PREV:_a'"
            name = shape["name"]

            # 1. projection + WHERE on an aligned column; the threshold splits
            # the aligned value set so NULL rows and filtered rows both occur.
            entries = parse_definition(defs1, cols)
            frame = build_frame(shape, {first}, False)
            aligned_values = sorted({row["align"][first] for row in frame if row["align"][first] is not None})
            threshold = aligned_values[len(aligned_values) // 2] if aligned_values else 0
            expected = []
            for row in frame:
                value = row["align"][first]
                if value is None or value < threshold:
                    continue
                expected.append(
                    ([row["site"], row["ts"], value] if tagged else [row["ts"], value])
                )
            _assert_query(
                f"select {site_sel}ts, {first_a} from "
                f"table(time_align(table {name}, {defs1})) "
                f"where {first_a} >= {threshold} order by {order}",
                expected,
            )

            # 2. ORDER BY DESC + LIMIT/OFFSET on the aligned row set.
            reversed_rows = list(reversed(frame))[1:3]
            expected = [
                ([row["site"], row["ts"], row["align"][first]] if tagged
                 else [row["ts"], row["align"][first]])
                for row in reversed_rows
            ]
            _assert_query(
                f"select {site_sel}ts, {first_a} from "
                f"table(time_align(table {name}, {defs1})) "
                f"order by {'site desc, ' if tagged else ''}ts desc limit 2 offset 1",
                expected,
            )

            # 3. Aggregates over the aligned column; tagged tables group by tag.
            if tagged:
                groups = {}
                for row in frame:
                    groups.setdefault(row["site"], []).append(row["align"][first])
                expected = [
                    [site] + _aggregate(groups[site]) for site in sorted(groups)
                ]
                sql = (
                    f"select site, count(*), max({first_a}), min({first_a}), sum({first_a}), avg({first_a}) "
                    f"from table(time_align(table {name}, {defs1})) group by site order by site"
                )
            else:
                expected = [_aggregate([row["align"][first] for row in frame])]
                sql = (
                    f"select count(*), max({first_a}), min({first_a}), sum({first_a}), avg({first_a}) "
                    f"from table(time_align(table {name}, {defs1}))"
                )
            _assert_query(sql, expected)

            # 4. INTERVAL windows over the aligned column; tagged tables use
            # PARTITION BY tag and therefore per-child windows.
            groups = {}
            for row in frame:
                window = (row["ts"] - BASE_TS) // WINDOW_MS
                key = (row["site"] if tagged else None, window)
                groups.setdefault(key, []).append(row["align"][first])
            expected = []
            for key in sorted(groups, key=lambda item: (item[0] if item[0] is not None else 0, item[1])):
                values = groups[key]
                nonnull = [value for value in values if value is not None]
                wstart = BASE_TS + key[1] * WINDOW_MS
                total = sum(nonnull) if nonnull else None
                expected.append(
                    ([key[0], wstart, len(values), total] if tagged
                     else [wstart, len(values), total])
                )
            if tagged:
                sql = (
                    f"select site, _wstart, count(*), sum({first_a}) from "
                    f"table(time_align(table {name}, {defs1})) "
                    f"partition by site interval(2s) order by site, _wstart"
                )
            else:
                sql = (
                    f"select _wstart, count(*), sum({first_a}) from "
                    f"table(time_align(table {name}, {defs1})) interval(2s) order by _wstart"
                )
            _assert_query(sql, expected)

            # 5. Expression over two aligned columns (NULL propagates).
            if second is not None:
                defs2 = f"'{first}:PREV:_a;{second}:PREV:_a'"
                frame2 = build_frame(shape, {first, second}, False)
                expected = []
                for row in frame2:
                    left = row["align"][first]
                    right = row["align"][second]
                    product = None if left is None or right is None else left * right
                    expected.append(
                        ([row["site"], row["ts"], product] if tagged
                         else [row["ts"], product])
                    )
                _assert_query(
                    f"select {site_sel}ts, {first_a} * {second_a} as product from "
                    f"table(time_align(table {name}, {defs2})) order by {order}",
                    expected,
                )

            # 6. output_raw=TRUE: raw columns precede aligned columns and WHERE
            # on a raw column filters rows without changing fill history before
            # the align stage.
            frame_raw = build_frame(shape, {first}, True)
            expected = [
                ([row["site"], row["ts"], row["raw"][first], row["align"][first]] if tagged
                 else [row["ts"], row["raw"][first], row["align"][first]])
                for row in frame_raw
                if row["raw"][first] is not None
            ]
            _assert_query(
                f"select {site_sel}ts, {first}, {first_a} from "
                f"table(time_align(table {name}, TRUE, {defs1})) "
                f"where {first} is not null order by {order}",
                expected,
            )

    def test_mixed_raw_aligned_matrix(self):
        """Raw columns and aligned columns computed in the same statement.

        output_raw=TRUE keeps the original columns untouched while the aligned
        columns carry PREV values.  Every query below mixes both in one project
        list, WHERE, aggregate or window, and is compared value by value with
        the model:

        - row-level: a raw NULL must stay NULL on the same row where its
          aligned column is filled from an earlier value
        - expressions: raw(col0) * aligned(col1) differs from
          aligned(col0) * raw(col1) exactly where one side has a raw NULL
        - WHERE: every non-NULL raw value equals its aligned copy, and
          `raw IS NULL AND aligned IS NOT NULL` selects precisely the filled rows
        - aggregates: count/sum/avg/max/min of the raw column and of the
          aligned column in the same SELECT (they differ whenever NULL gaps
          exist), overall and per tag
        - INTERVAL windows: raw and aligned counts/sums side by side, overall
          and PARTITION BY tag
        - tag-filtered raw+aligned projection

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, matrix, mixed, ci
        """
        for shape in SHAPES.values():
            cols = shape["columns"]
            first = cols[0]
            first_a = f"{first}_a"
            tagged = _shape_has_site(shape)
            site_sel = "site, " if tagged else ""
            order = "site, ts" if tagged else "ts"
            defs = "TRUE, '*::_a'"
            name = shape["name"]
            frame = build_frame(shape, set(cols), True)
            pairs = ", ".join(f"{col}, {col}_a" for col in cols)

            # 1. row-level raw + aligned projection: raw NULL is preserved,
            # aligned is filled on the same row.
            expected = []
            for row in frame:
                out = [row["site"], row["ts"]] if tagged else [row["ts"]]
                for col in cols:
                    out.extend([row["raw"][col], row["align"][col]])
                expected.append(out)
            _assert_query(
                f"select {site_sel}ts, {pairs} from "
                f"table(time_align(table {name}, {defs})) order by {order}",
                expected,
            )

            # 2. mixed expressions: raw on one side, aligned on the other.
            if len(cols) >= 2:
                second = cols[1]
                expected = []
                for row in frame:
                    raw_first = row["raw"][first]
                    raw_second = row["raw"][second]
                    aligned_first = row["align"][first]
                    aligned_second = row["align"][second]
                    left = None if raw_first is None or aligned_second is None else raw_first * aligned_second
                    right = None if aligned_first is None or raw_second is None else aligned_first * raw_second
                    expected.append(
                        ([row["site"], row["ts"], left, right] if tagged
                         else [row["ts"], left, right])
                    )
                _assert_query(
                    f"select {site_sel}ts, {first} * {second}_a as raw_times_aligned, "
                    f"{first}_a * {second} as aligned_times_raw from "
                    f"table(time_align(table {name}, {defs})) order by {order}",
                    expected,
                )

            # 3. mixed WHERE:
            #   a) non-NULL raw equals its aligned copy, NULL raw rows are the
            #      rows filled by PREV -> the compound predicate selects both.
            condition_rows = []
            for row in frame:
                raw_value = row["raw"][first]
                aligned_value = row["align"][first]
                matched = (raw_value is None and aligned_value is not None) or (
                    raw_value is not None and raw_value == aligned_value
                )
                if matched:
                    condition_rows.append(
                        ([row["site"], row["ts"], raw_value, aligned_value] if tagged
                         else [row["ts"], raw_value, aligned_value])
                    )
            _assert_query(
                f"select {site_sel}ts, {first}, {first_a} from "
                f"table(time_align(table {name}, {defs})) "
                f"where ({first} is null and {first_a} is not null) or "
                f"({first} is not null and {first} = {first_a}) order by {order}",
                condition_rows,
            )

            #   b) a sampled (non-NULL) raw value never differs from its aligned
            #      copy -> no rows.
            _assert_query(
                f"select {site_sel}ts, {first}, {first_a} from "
                f"table(time_align(table {name}, {defs})) "
                f"where {first} is not null and {first} <> {first_a} order by {order}",
                [],
                expected_cols=(4 if tagged else 3),
            )

            #   c) every raw NULL row still projects its aligned counterpart.
            expected = []
            for row in frame:
                if row["raw"][first] is None:
                    expected.append(
                        ([row["site"], row["ts"], None, row["align"][first]] if tagged
                         else [row["ts"], None, row["align"][first]])
                    )
            _assert_query(
                f"select {site_sel}ts, {first}, {first_a} from "
                f"table(time_align(table {name}, {defs})) "
                f"where {first} is null order by {order}",
                expected,
            )

            # 4. raw vs aligned aggregates in the same SELECT, per column.
            for col in cols:
                col_a = f"{col}_a"
                select_list = (
                    f"count(*), count({col}), sum({col}), avg({col}), max({col}), min({col}), "
                    f"count({col_a}), sum({col_a}), avg({col_a}), max({col_a}), min({col_a})"
                )
                if tagged:
                    groups = {}
                    for row in frame:
                        groups.setdefault(row["site"], []).append(row)
                    expected = []
                    for site in sorted(groups):
                        rows = groups[site]
                        expected.append(
                            [site, len(rows)]
                            + _nonnull_stats([row["raw"][col] for row in rows])
                            + _nonnull_stats([row["align"][col] for row in rows])
                        )
                    sql = (
                        f"select site, {select_list} from "
                        f"table(time_align(table {name}, {defs})) group by site order by site"
                    )
                else:
                    expected = [[len(frame)]
                                + _nonnull_stats([row["raw"][col] for row in frame])
                                + _nonnull_stats([row["align"][col] for row in frame])]
                    sql = (
                        f"select {select_list} from "
                        f"table(time_align(table {name}, {defs}))"
                    )
                _assert_query(sql, expected)

            # 5. raw vs aligned counts/sums in the same INTERVAL window, overall
            # and per tag.
            groups = {}
            for row in frame:
                window = (row["ts"] - BASE_TS) // WINDOW_MS
                key = (row["site"] if tagged else None, window)
                groups.setdefault(key, []).append(row)
            expected = []
            for key in sorted(groups, key=lambda item: (item[0] if item[0] is not None else 0, item[1])):
                rows = groups[key]
                raw_values = [row["raw"][first] for row in rows]
                aligned_values = [row["align"][first] for row in rows]
                raw_nonnull = [value for value in raw_values if value is not None]
                aligned_nonnull = [value for value in aligned_values if value is not None]
                wstart = BASE_TS + key[1] * WINDOW_MS
                row = ([key[0], wstart] if tagged else [wstart]) + [
                    len(rows),
                    len(raw_nonnull),
                    len(aligned_nonnull),
                    sum(raw_nonnull) if raw_nonnull else None,
                    sum(aligned_nonnull) if aligned_nonnull else None,
                ]
                expected.append(row)
            if tagged:
                sql = (
                    f"select site, _wstart, count(*), count({first}), count({first_a}), "
                    f"sum({first}), sum({first_a}) from "
                    f"table(time_align(table {name}, {defs})) "
                    f"partition by site interval(2s) order by site, _wstart"
                )
            else:
                sql = (
                    f"select _wstart, count(*), count({first}), count({first_a}), "
                    f"sum({first}), sum({first_a}) from "
                    f"table(time_align(table {name}, {defs})) interval(2s) order by _wstart"
                )
            _assert_query(sql, expected)

            # 6. tag filter keeps both raw and aligned columns correct per child.
            if tagged:
                site = sorted({row["site"] for row in frame})[-1]
                expected = []
                for row in frame:
                    if row["site"] != site:
                        continue
                    out = [row["site"], row["ts"]]
                    for col in cols:
                        out.extend([row["raw"][col], row["align"][col]])
                    expected.append(out)
                _assert_query(
                    f"select site, ts, {pairs} from "
                    f"table(time_align(table {name}, {defs})) "
                    f"where site = {site} order by site, ts",
                    expected,
                )

    def test_tag_matrix(self):
        """Tag propagation across virtual super table, child table and owned tags.

        - vstable output_raw=TRUE appends tag columns after the aligned columns
        - virtual child table output_raw=TRUE does not output its tags, but the
          tag remains selectable and filterable
        - virtual normal table owned tags are not output either, but are
          selectable/filterable
        - vstable children keep independent PREV state after tag filtering

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, matrix, tags, ci
        """
        # vstable: tags are the last output columns
        shape = SHAPES["vst_disjoint"]
        entries = parse_definition("*:PREV:_a", shape["columns"])
        expected = natural_rows(shape, entries, True)
        _assert_query(
            "select * from table(time_align(table vst_disjoint, TRUE, '*:PREV:_a')) "
            "order by site, ts",
            expected,
            expected_cols=1 + len(shape["columns"]) * 2 + 1,
        )

        # virtual child table: tag not output, but selectable/filterable
        shape = SHAPES["vd1"]
        expected = natural_rows(shape, entries, True)
        _assert_query(
            "select * from table(time_align(table vd1, TRUE, '*:PREV:_a')) order by ts",
            expected,
            expected_cols=1 + len(shape["columns"]) * 2,
        )
        frame = build_frame(shape, set(shape["columns"]), True)
        expected = [[row["site"], row["ts"], row["align"]["voltage"], row["align"]["current"]]
                    for row in frame if row["site"] == 1]
        _assert_query(
            "select site, ts, voltage_a, current_a from "
            "table(time_align(table vd1, '*:PREV:_a')) where site = 1 order by site, ts",
            expected,
        )

        # tbname pseudo column selects a single child of the vstable.
        frame = build_frame(SHAPES["vd1"], {"voltage"}, False)
        expected = [[row["ts"], row["align"]["voltage"]] for row in frame]
        _assert_query(
            "select ts, voltage_a from "
            "table(time_align(table vst_disjoint, 'voltage:PREV:_a')) "
            "where tbname = 'vd1' order by ts",
            expected,
        )

        # owned tags on a virtual normal table: not output, selectable
        shape = SHAPES["vt_ntag"]
        frame = build_frame(shape, {"voltage"}, False)
        expected = [["alpha", 7, row["ts"], row["align"]["voltage"]] for row in frame]
        _assert_query(
            "select name, cnt, ts, voltage_a from "
            "table(time_align(table vt_ntag, '*:PREV:_a')) "
            "where name = 'alpha' and cnt = 7 order by ts",
            expected,
        )
        tdSql.query("select * from table(time_align(table vt_ntag, TRUE, '*:PREV:_a')) order by ts")
        assert tdSql.queryCols == 1 + len(shape["columns"]) * 2, (
            f"owned tags must not be part of the output: {tdSql.queryCols} columns"
        )

        # overlapping children: PREV state resets per child even when the tag
        # filter selects one child; o2's first current is NULL and must stay NULL.
        shape = SHAPES["vst_overlap"]
        frame = build_frame(shape, set(shape["columns"]), False)
        expected = [[row["site"], row["ts"], row["align"]["voltage"], row["align"]["current"]]
                    for row in frame if row["site"] == 2]
        _assert_query(
            "select site, ts, voltage_a, current_a from "
            "table(time_align(table vst_overlap, '*:PREV:_a')) where site = 2 order by site, ts",
            expected,
        )
        assert expected[0][3] is None, "PREV must not leak across vstable children"

    def test_all_data_types(self):
        """Every user-creatable column type goes through PREV unchanged.

        Creates one source table and one virtual table containing BOOL,
        TIMESTAMP, all signed/unsigned integer widths, FLOAT, DOUBLE, BINARY,
        NCHAR, VARBINARY, GEOMETRY, DECIMAL64 and DECIMAL128.  The source has
        four rows: values, all-NULL, other values, all-NULL.  PREV must return
        the values for every type (NULL runs take the latest non-NULL value)
        both in the aligned-only output and in output_raw=TRUE output.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, datatype, ci
        """
        type_columns = [
            ("c_bool", "bool"),
            ("c_ts", "timestamp"),
            ("c_tint", "tinyint"),
            ("c_utint", "tinyint unsigned"),
            ("c_sint", "smallint"),
            ("c_usint", "smallint unsigned"),
            ("c_int", "int"),
            ("c_uint", "int unsigned"),
            ("c_bint", "bigint"),
            ("c_ubint", "bigint unsigned"),
            ("c_float", "float"),
            ("c_double", "double"),
            ("c_bin", "binary(8)"),
            ("c_nchar", "nchar(8)"),
            ("c_vbin", "varbinary(8)"),
            ("c_geo", "geometry(64)"),
            ("c_dec", "decimal(10,2)"),
            ("c_dec38", "decimal(38,4)"),
        ]
        null_cells = ", ".join("NULL" for _ in type_columns)

        tdSql.execute("drop vtable if exists vt_all_types")
        tdSql.execute("drop table if exists t_all_types")
        column_ddl = ", ".join(f"{name} {typ}" for name, typ in type_columns)
        tdSql.execute(f"create table t_all_types (ts timestamp, {column_ddl})")
        tdSql.execute(
            f"insert into t_all_types values "
            f"({ts(0)}, true, {ts(0)}, 1, 11, 101, 1001, 10001, 100001, 1000001, 10000001, "
            f"1.5, 10.5, 'abc', 'nchar', 'vbin', \"POINT(1.0 1.0)\", "
            f"123.45, 1234567890123456789012345678.1234) "
            f"({ts(1000)}, {null_cells}) "
            f"({ts(2000)}, false, {ts(2000)}, -1, 12, -101, 1002, -10001, 100002, "
            f"-1000001, 10000002, 2.5, 20.5, 'def', 'nchar2', 'vbin2', \"POINT(2.0 2.0)\", "
            f"-123.45, -1234567890123456789012345678.1234) "
            f"({ts(3000)}, {null_cells})"
        )
        vtable_columns = ", ".join(
            f"{name} {typ} from t_all_types.{name}" for name, typ in type_columns
        )
        tdSql.execute(f"create vtable vt_all_types (ts timestamp, {vtable_columns})")

        # The raw query is the reference for every type (bytes, Decimal and
        # datetime compare naturally in Python), so no type literal is
        # duplicated in the expectations.
        tdSql.query("select * from t_all_types order by ts")
        raw_rows = [list(row) for row in tdSql.queryResult]
        tdSql.checkRows(4)

        last = [None] * len(raw_rows[0])
        expected = []
        for row in raw_rows:
            current = [row[0]]
            for index in range(1, len(row)):
                if row[index] is not None:
                    last[index] = row[index]
                current.append(last[index])
            expected.append(current)

        tdSql.query("select * from table(time_align(table vt_all_types))")
        tdSql.checkRows(4)
        assert tdSql.queryCols == 1 + len(type_columns), (
            f"expected {1 + len(type_columns)} columns, got {tdSql.queryCols}"
        )
        aligned_rows = [list(row) for row in tdSql.queryResult]
        for row_index in range(4):
            for col_index in range(len(type_columns) + 1):
                assert aligned_rows[row_index][col_index] == expected[row_index][col_index], (
                    f"row {row_index} col {col_index}: "
                    f"{aligned_rows[row_index][col_index]!r} != {expected[row_index][col_index]!r}"
                )

        # output_raw=TRUE: raw columns first, aligned columns after, all types.
        tdSql.query(
            "select * from table(time_align(table vt_all_types, TRUE, '*::_a')) order by ts"
        )
        tdSql.checkRows(4)
        assert tdSql.queryCols == 1 + len(type_columns) * 2, (
            f"expected {1 + len(type_columns) * 2} columns, got {tdSql.queryCols}"
        )
        raw_aligned_rows = [list(row) for row in tdSql.queryResult]
        for row_index in range(4):
            for col_index, _ in enumerate(type_columns):
                raw_value = raw_aligned_rows[row_index][1 + col_index]
                aligned_value = raw_aligned_rows[row_index][1 + len(type_columns) + col_index]
                assert raw_value == raw_rows[row_index][1 + col_index], (
                    f"raw row {row_index} col {col_index} mismatch"
                )
                assert aligned_value == expected[row_index][1 + col_index], (
                    f"aligned row {row_index} col {col_index}: "
                    f"{aligned_value!r} != {expected[row_index][1 + col_index]!r}"
                )

        # WHERE on a GEOMETRY/DECIMAL aligned column uses the aligned values.
        _assert_query(
            "select c_bool_a, c_geo_a, c_dec38_a from "
            "table(time_align(table vt_all_types, '*::_a')) "
            "where c_geo_a is not null order by ts",
            [[expected[row][1], expected[row][16], expected[row][18]] for row in range(4)],
            expected_cols=3,
        )

    def test_table_function_positions(self):
        """Invalid TABLE(...) contents and TIME_ALIGN outside table positions.

        `TABLE(...)` only accepts `TIME_ALIGN([db.]table [, args])` (the
        legacy `TIME_ALIGN(TABLE [db.]table [, args])` form is also accepted);
        anything else (another function, raw values, a missing table name or
        malformed argument lists) is a syntax error.

        Using `time_align` as a scalar function reports function-not-defined,
        and the FS 4.8.4 unsupported positions are rejected: INSERT ... SELECT
        and CREATE STREAM with a dedicated usage error, CREATE TOPIC by the
        topic-query validation, and DDL/DELETE by the grammar.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, negative, position, ci
        """
        # 1. TABLE(...) with something other than TIME_ALIGN(TABLE ...).
        invalid_calls = [
            "select * from table(nosuchfunc(table vt_gaps))",
            "select * from table(count(table vt_gaps))",
            "select * from table(time_align(1))",
            "select * from table(time_align('abc'))",
            "select * from table(time_align())",
            "select * from table(time_align(table))",
            "select * from table()",
            "select * from table(123)",
            "select * from table('abc')",
            "select * from table(nosuchfunc())",
            "select * from table(time_align(table vt_gaps, 1.5))",
            "select * from table(time_align(table vt_gaps, true, true))",
            "select * from table(time_align(table vt_gaps, true, 'a', true))",
            "select * from table(time_align(table vt_gaps, TRUE, 'voltage:PREV:_p', 'x'))",
            "select * from time_align(table vt_gaps)",
            # Only a table name may follow TABLE; other functions are rejected
            # by the grammar or by the explicit "unsupported table function"
            # check.
            "select * from table(csum(ts))",
            "select * from table(avg(voltage))",
            "select * from table(count(voltage))",
            "select * from table(sum(table vt_gaps))",
            "select * from table(time_align(table (select ts, voltage from vt_gaps), 'voltage:PREV:_p'))",
            # FS 4.8.3.5: the TIME_ALIGN result cannot be a TIME_ALIGN input;
            # the TABLE(...) argument is not a table source.
            "select * from table(time_align(table table(time_align(table vt_gaps))))",
        ]
        for sql in invalid_calls:
            tdSql.error(sql, PAR_SYNTAX_ERROR)

        # 2. The TABLE(...) form is only valid in the FROM clause.  In any
        #    other clause the TABLE keyword makes the statement a syntax error.
        for sql in (
            "select time_align(table vt_gaps) from vt_gaps",
            "select ts from vt_gaps where time_align(table vt_gaps) is not null",
            "select ts from vt_gaps group by time_align(table vt_gaps)",
            "select ts from vt_gaps having time_align(table vt_gaps) is not null",
            "select ts from vt_gaps order by time_align(table vt_gaps)",
            "select ts from vt_gaps partition by time_align(table vt_gaps)",
            "select 1 from t_gaps a join t_gaps b on time_align(table t_gaps) is not null",
        ):
            tdSql.error(sql, PAR_SYNTAX_ERROR)

        # 3. time_align as a scalar function outside FROM.
        for sql in (
            "select time_align(1)",
            "select time_align(ts) from t_gaps",
            "select * from t_gaps where time_align(ts) > 0",
            "select ts from t_gaps having time_align(ts) > 0",
            "select ts from t_gaps partition by time_align(ts)",
            "select 1 from t_gaps a join t_gaps b on time_align(a.ts) > 0",
            "select * from table(time_align(table t_gaps)) where time_align(ts) > 0",
        ):
            tdSql.error(sql, MND_FUNC_NOT_EXIST)
        tdSql.error("select time_align(ts) from t_gaps group by time_align(ts)",
                    MND_FUNC_NOT_EXIST)
        tdSql.error("select * from t_gaps order by time_align(ts)",
                    [MND_FUNC_NOT_EXIST, PAR_FUNC_NOT_EXIST_ORDERBY])
        tdSql.error("insert into t_gaps values(now, time_align(1), 1, 1)", 0x80000216)

        # 3. FS 4.8.4 unsupported statement positions.
        tdSql.error(
            "insert into t_gaps select * from table(time_align(table vt_gaps))",
            PAR_INVALID_OPTR_USAGE)
        tdSql.error(
            "create stream ta_s interval(1s) from t_gaps into ta_s_out as "
            "select * from table(time_align(table vt_gaps))",
            PAR_INVALID_OPTR_USAGE)
        tdSql.error(
            "create topic ta_topic as select * from table(time_align(table vt_gaps))",
            VTABLE_TOPIC_REJECT)
        tdSql.error(
            "create vtable v_bad (ts timestamp, voltage double from "
            "table(time_align(table vt_gaps)))",
            PAR_SYNTAX_ERROR)
        tdSql.error(
            "alter vtable vt_gaps add column extra int from "
            "table(time_align(table vt_gaps))",
            PAR_SYNTAX_ERROR)
        tdSql.error("delete from table(time_align(table vt_gaps))", PAR_SYNTAX_ERROR)
        # Writing the table function result is not a supported DDL position
        # either; CREATE TABLE ... AS is not part of the grammar.
        tdSql.error(
            "create table ta_align_cas as select * from "
            "table(time_align(table vt_gaps))",
            PAR_SYNTAX_ERROR)

    def test_invalid_definitions(self):
        """Invalid definitions are rejected with the documented error codes.

        Covers argument forms, definition grammar (including quoted
        identifiers and separators), name conflicts and references to source
        columns that the output does not expose.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, negative, ci
        """
        # NEXT is implemented by phase 2; its positive/negative coverage lives
        # in test_time_align_next.py.
        # The primary timestamp and tags cannot be interpolated; both the
        # vstable tag and the owned tag of a virtual normal table are covered.
        tdSql.error(
            "select * from table(time_align(table vt_gaps, 'ts:PREV:_a'))",
            PAR_INVALID_COLUMN)
        tdSql.error(
            "select * from table(time_align(table vst_disjoint, 'site:PREV:_a'))",
            PAR_INVALID_COLUMN)
        tdSql.error(
            "select * from table(time_align(table vt_ntag, 'name:PREV:_a'))",
            PAR_INVALID_COLUMN)
        # Unknown source column.
        tdSql.error(
            "select * from table(time_align(table vt_gaps, 'nocol:PREV:_a'))",
            PAR_INVALID_COLUMN)
        # Duplicate output name, including the default-name conflict with raw
        # output: TRUE without an explicit suffix gives 'voltage' twice.
        tdSql.error(
            "select * from table(time_align(table vt_gaps, 'voltage:PREV:_a;voltage:PREV:_a'))",
            PAR_DUPLICATED_COLUMN)
        tdSql.error(
            "select * from table(time_align(table vt_gaps, '*::_a;*::_a'))",
            PAR_DUPLICATED_COLUMN)
        tdSql.error(
            "select * from table(time_align(table vt_gaps, TRUE))",
            PAR_DUPLICATED_COLUMN)
        tdSql.error(
            "select * from table(time_align(table vt_gaps, TRUE, 'voltage:PREV:'))",
            PAR_DUPLICATED_COLUMN)
        # FS 4.4.3: different sources can still produce the same final name
        # (a::_b -> a_b and source column a_b -> a_b).
        tdSql.error(
            "select * from table(time_align(table vt_coll, 'a::_b;a_b'))",
            PAR_DUPLICATED_COLUMN)
        # FS 4.4.3: an aligned output may never collide with a tag, even when
        # output_raw=FALSE (raw columns are not output then).
        tdSql.error(
            "select * from table(time_align(table vt_tagconflict, 'v:PREV:oltage'))",
            PAR_DUPLICATED_COLUMN)
        tdSql.error(
            "select * from table(time_align(table vt_tagconflict, TRUE, 'v:PREV:oltage'))",
            PAR_DUPLICATED_COLUMN)
        # FS 4.5.1: the primary timestamp is always output exactly once;
        # a generated name that reproduces it is rejected in both modes.
        tdSql.error(
            "select * from table(time_align(table vt_pkname, 't:PREV:s'))",
            PAR_DUPLICATED_COLUMN)
        tdSql.error(
            "select * from table(time_align(table vt_pkname, TRUE, 't:PREV:s'))",
            PAR_DUPLICATED_COLUMN)
        # Unknown method: a lone suffix without '::' is parsed as the method name.
        tdSql.error(
            "select * from table(time_align(table vt_gaps, 'voltage:FOO:_a'))",
            PAR_NOT_ALLOWED_FUNC)
        tdSql.error(
            "select * from table(time_align(table vt_gaps, 'voltage:_a'))",
            PAR_NOT_ALLOWED_FUNC)
        # A suffix cannot contain ':'.
        tdSql.error(
            "select * from table(time_align(table vt_gaps, 'voltage:PREV:_a:_b'))",
            PAR_SYNTAX_ERROR)
        # FS 4.2 note 6: only an omitted definition defaults to '*:PREV:';
        # an explicitly supplied empty string is invalid.
        tdSql.error(
            "select * from table(time_align(table vt_gaps, ''))",
            PAR_SYNTAX_ERROR)
        tdSql.error(
            "select * from table(time_align(table vt_gaps, TRUE, ''))",
            PAR_SYNTAX_ERROR)
        # Empty definitions created by separators, including a trailing ';'.
        for bad_defs in ("';'", "' ; '", "':PREV:_a'",
                         "'voltage:PREV:_a;;current:PREV:_b'",
                         "'voltage:PREV:_a;'"):
            tdSql.error(
                f"select * from table(time_align(table vt_gaps, {bad_defs}))",
                PAR_SYNTAX_ERROR)
        # The final name must fit into a column name.
        tdSql.error(
            "select * from table(time_align(table vt_gaps, "
            f"'voltage:PREV:{'x' * 70}'))",
            PAR_INVALID_COLUMN)
        # output_raw=FALSE exposes only ts + aligned columns + tags.  A source
        # data column is not part of the output and must not leak through
        # (otherwise e.g. PARTITION BY/sum() would read unaligned values).
        tdSql.error(
            "select sum(voltage) from table(time_align(table vt_gaps, 'voltage:PREV:_a'))",
            PAR_INVALID_COLUMN)
        tdSql.error(
            "select status from table(time_align(table vt_gaps, '*:PREV:_a')) "
            "partition by status",
            PAR_INVALID_COLUMN)
        tdSql.error(
            "select ts from table(time_align(table vt_gaps, 'voltage:PREV:_a')) "
            "where current > 1",
            PAR_INVALID_COLUMN)
        tdSql.error(
            "select ts from table(time_align(table vt_gaps, 'voltage:PREV:_a')) "
            "order by current",
            PAR_INVALID_COLUMN)
        tdSql.error(
            "select ts from table(time_align(table vt_gaps, 'voltage:PREV:_a')) "
            "group by current",
            PAR_INVALID_COLUMN)
        # Argument type mismatches are grammar errors.
        tdSql.error(
            "select * from table(time_align(table vt_gaps, 123))",
            PAR_SYNTAX_ERROR)
        tdSql.error(
            "select * from table(time_align(table vt_gaps, TRUE, 123))",
            PAR_SYNTAX_ERROR)
        # FS 4.2 note 5: arguments are matched by position and literal type;
        # a column definition before output_raw, or two definitions, is invalid.
        tdSql.error(
            "select * from table(time_align(table vt_gaps, '*::_a', TRUE))",
            PAR_SYNTAX_ERROR)
        tdSql.error(
            "select * from table(time_align(table vt_gaps, '*::_a', '*::_b'))",
            PAR_SYNTAX_ERROR)
        # A string in the output_raw position is a column definition, not a
        # boolean, so the unknown column is reported.
        tdSql.error(
            "select * from table(time_align(table vt_gaps, 'TRUE'))",
            PAR_INVALID_COLUMN)

    def test_system_table_rejected(self):
        """System tables and missing tables stay rejected in phase 2.

        Ordinary/child/super tables and views are supported by phase 2 and are
        covered by test_time_align_phase2.py; system tables and nonexistent
        tables keep their existing errors (FS 4.10, DS 6.12).

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, negative, ci
        """
        tdSql.error(
            "select * from table(time_align(table information_schema.ins_databases))",
            PAR_INVALID_TABLE_TYPE)
        tdSql.error(
            "select * from table(time_align(table information_schema.ins_tables))",
            PAR_INVALID_TABLE_TYPE)
        tdSql.error(
            "select * from table(time_align(table performance_schema.perf_trans))",
            PAR_INVALID_TABLE_TYPE)
        # The column-definition form is rejected by the same table-type check.
        tdSql.error(
            "select * from table(time_align(table information_schema.ins_tables, "
            "'table_name:PREV:_a'))", PAR_INVALID_TABLE_TYPE)
        tdSql.error(
            "select * from table(time_align(table ta_align_no_such_table))",
            PAR_TABLE_NOT_EXIST)

    def test_quoted_identifiers(self):
        """Quoted names: ':' and ';' inside backticks are literal.

        FS 4.2 note 10: separator scanning ignores ':' and ';' inside quoted
        identifiers, doubled backticks escape a backtick, and a suffix cannot
        contain ':' or ';' even when quoted.  An unterminated quote is a
        syntax error.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, negative, syntax, ci
        """
        _assert_query(
            "select ts, `a:b_x` from table(time_align(table vt_quote, "
            "'`a:b`:PREV:_x')) order by ts",
            [[ts(0), 1], [ts(1000), 3]])
        _assert_query(
            "select ts, `c;d_y` from table(time_align(table vt_quote, "
            "'`c;d`::_y')) order by ts",
            [[ts(0), 2], [ts(1000), 4]])
        _assert_query(
            "select ts, `a:b_z`, `c;d_w` from table(time_align(table vt_quote, "
            "'`a:b`::_z;`c;d`::_w')) order by ts",
            [[ts(0), 1, 2], [ts(1000), 3, 4]])
        _assert_query(
            "select ts, `a``b_x` from table(time_align(table vt_bt, "
            "'`a``b`:PREV:_x')) order by ts",
            [[ts(0), 7], [ts(1000), 8]])

        # Missing closing backtick.
        tdSql.error(
            "select * from table(time_align(table vt_quote, '`a:b:PREV:_x'))",
            PAR_SYNTAX_ERROR)
        # The suffix may not contain the separators, even quoted.
        tdSql.error(
            "select * from table(time_align(table vt_quote, '`a:b`:PREV:`x:y`'))",
            PAR_SYNTAX_ERROR)
        tdSql.error(
            "select * from table(time_align(table vt_quote, '`a:b`:PREV:`x;y`'))",
            PAR_SYNTAX_ERROR)
        # Unquoted ':' is a method separator, not part of the column name.
        tdSql.error(
            "select * from table(time_align(table vt_quote, 'a:b'))",
            PAR_NOT_ALLOWED_FUNC)

    def test_select_permission(self):
        """SELECT privilege on the input vtable is enforced.

        FS 6.1: TIME_ALIGN must use the same privilege model as querying the
        input table directly.  A user with only USE on the database is denied
        for both the direct vtable query and the table function; after GRANT
        SELECT on the vtable both succeed, and a later REVOKE denies again.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, negative, auth, ci
        """
        user = "ta_align_perm"
        password = "test12@#*"
        tdSql.execute(f"drop user if exists {user}")
        tdSql.execute(f"create user {user} pass '{password}'")
        tdSql.execute(f"grant use on database {DB} to {user}")
        tdSql.execute("reset query cache")

        kwargs = {"user": user, "password": password}
        if getattr(self, "host", None):
            kwargs["host"] = self.host
        if getattr(self, "port", None):
            kwargs["port"] = self.port

        conn = taos.connect(**kwargs)
        try:
            limited = TDSql()
            limited.init(conn.cursor())
            limited.execute(f"use {DB}")
            for sql in (
                "select * from vt_gaps limit 1",
                "select * from table(time_align(table vt_gaps)) limit 1",
            ):
                limited.error(sql, PAR_PERMISSION_DENIED)

            tdSql.execute(f"grant select on table {DB}.vt_gaps to {user}")
            tdSql.execute("reset query cache")
            limited.query("select count(*) from vt_gaps")
            limited.checkData(0, 0, len(SOURCE_ROWS["t_gaps"]))
            limited.query("select count(*) from table(time_align(table vt_gaps))")
            limited.checkData(0, 0, len(SOURCE_ROWS["t_gaps"]))

            tdSql.execute(f"revoke select on table {DB}.vt_gaps from {user}")
            tdSql.execute("reset query cache")
            limited.error(
                "select * from table(time_align(table vt_gaps)) limit 1",
                PAR_PERMISSION_DENIED)
        finally:
            conn.close()
            tdSql.execute(f"drop user if exists {user}")
    def test_time_range_prev(self):
        """PREV(duration) bounds how far the predecessor value may be.

        Only NULL rows are interpolated; a row that has its own value is never
        replaced.  The distance is a closed interval, fixed units are converted
        with the input time-axis precision, calendar units (n/q/y) advance with
        TDengine's calendar rules, and an omitted duration keeps the unlimited
        behaviour.  The same semantics apply to virtual tables, views (time axis
        = first TIMESTAMP data column) and real tables.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: virtual, time_align, time_range, ci
        """
        import datetime

        def utc_ms(year, month, day):
            return int(datetime.datetime(year, month, day, 12, tzinfo=datetime.timezone.utc).timestamp() * 1000)

        def expect_second_col(sql, expected):
            tdSql.query(sql)
            tdSql.checkRows(len(expected))
            for i, value in enumerate(expected):
                tdSql.checkData(i, 1, value)

        def insert_rows(table, offsets, values):
            cells = " ".join(
                f"({offset}, {'NULL' if value is None else value})" for offset, value in zip(offsets, values))
            tdSql.execute(f"insert into {table} values {cells}")

        # Value at 0s, a gap, a value at 4s and a gap to 9s.  Rows 1..3 are
        # 1s/2s/3s away from the first value, so the 2s closed interval keeps
        # only the first two; row 3 keeps its own value at any distance.
        offsets = [0, 1000, 2000, 3000, 4000, 5000, 9000]
        values = [1, None, None, None, 9, None, None]
        tdSql.execute("create table tr_prev(ts timestamp, v int)")
        insert_rows("tr_prev", [ts(o) for o in offsets], values)

        expect_second_col(
            "select ts, v_p from table(time_align(table tr_prev, 'v:PREV(2s):_p')) order by ts",
            [1, 1, 1, None, 9, 9, None])
        expect_second_col(
            "select ts, v_p from table(time_align(table tr_prev, 'v:PREV(1s):_p')) order by ts",
            [1, 1, None, None, 9, 9, None])
        # Closed interval: a distance of exactly 3s still uses the value.
        expect_second_col(
            "select ts, v_p from table(time_align(table tr_prev, 'v:PREV(3s):_p')) order by ts",
            [1, 1, 1, 1, 9, 9, None])
        # PREV(0s) only accepts a candidate on the same timestamp.
        expect_second_col(
            "select ts, v_p from table(time_align(table tr_prev, 'v:PREV(0s):_p')) order by ts",
            [1, None, None, None, 9, None, None])
        # Omitted duration keeps the unlimited behaviour.
        expect_second_col(
            "select ts, v_p from table(time_align(table tr_prev, 'v:PREV:_p')) order by ts",
            [1, 1, 1, 1, 9, 9, 9])

        # '*' expansion applies the same range to every data column.
        expect_second_col(
            "select ts, v_s from table(time_align(table tr_prev, '*:PREV(2s):_s')) order by ts",
            [1, 1, 1, None, 9, 9, None])

        # Independent ranges for the same source column.
        tdSql.query("select ts, v_1, v_5 from table(time_align(table tr_prev, "
                    "'v:PREV(1s):_1;v:PREV(5s):_5')) order by ts")
        tdSql.checkRows(7)
        expect_1s = [1, 1, None, None, 9, 9, None]
        expect_5s = [1, 1, 1, 1, 9, 9, 9]
        for i in range(7):
            tdSql.checkData(i, 1, expect_1s[i])
            tdSql.checkData(i, 2, expect_5s[i])

        # The outer WHERE runs after interpolation; filtered-out rows still
        # feed the predecessor and never shorten the range.
        tdSql.query("select ts, v_p from table(time_align(table tr_prev, 'v:PREV(2s):_p')) "
                    f"where ts >= {ts(3000)} order by ts")
        tdSql.checkRows(4)
        for i, value in enumerate([None, 9, 9, None]):
            tdSql.checkData(i, 1, value)

        # Case-insensitive unit and blanks around the duration.
        for method in ("v:PREV(2S):_p", "v:PREV( 2s ):_p", "v:PREV (2s):_p"):
            expect_second_col(
                f"select ts, v_p from table(time_align(table tr_prev, '{method}')) order by ts",
                [1, 1, 1, None, 9, 9, None])

        # Fixed units: the same shape measured in each unit.
        for unit, step in (("a", 1), ("s", 1000), ("m", 60000), ("h", 3600000),
                           ("d", 86400000), ("w", 604800000)):
            table = f"tr_u_{unit}"
            tdSql.execute(f"create table {table}(ts timestamp, v int)")
            insert_rows(table, [ts(i * step) for i in (0, 1, 2, 3, 4, 5, 8)], values)
            expect_second_col(
                f"select ts, v_p from table(time_align(table {table}, 'v:PREV(2{unit}):_p')) order by ts",
                [1, 1, 1, None, 9, 9, None])

        # Units below the input precision convert to 0 ticks: only a candidate
        # on the same timestamp would qualify, and all timestamps differ here.
        tdSql.execute("create table tr_u_sub(ts timestamp, v int)")
        insert_rows("tr_u_sub", [ts(i) for i in (0, 1, 2, 3, 4, 5, 8)], values)
        for unit in ("b", "u"):
            expect_second_col(
                f"select ts, v_p from table(time_align(table tr_u_sub, 'v:PREV(2{unit}):_p')) order by ts",
                [1, None, None, None, 9, None, None])

        # Calendar units advance with taosTimeAdd(): q is three months and the
        # boundary is far enough from the sampled rows for any session timezone.
        tdSql.execute("create table tr_cal(ts timestamp, v int)")
        cal_rows = [
            (utc_ms(2023, 3, 15), 7),
            (utc_ms(2023, 4, 12), None),
            (utc_ms(2023, 4, 18), None),
            (utc_ms(2023, 6, 12), None),
            (utc_ms(2023, 6, 18), None),
            (utc_ms(2023, 9, 14), 8),
            (utc_ms(2023, 12, 11), None),
            (utc_ms(2023, 12, 18), None),
        ]
        insert_rows("tr_cal", [r[0] for r in cal_rows], [r[1] for r in cal_rows])
        expect_second_col(
            "select ts, v_p from table(time_align(table tr_cal, 'v:PREV(1n):_p')) order by ts",
            [7, 7, None, None, None, 8, None, None])
        expect_second_col(
            "select ts, v_p from table(time_align(table tr_cal, 'v:PREV(3n):_p')) order by ts",
            [7, 7, 7, 7, None, 8, 8, None])
        expect_second_col(
            "select ts, v_p from table(time_align(table tr_cal, 'v:PREV(1q):_p')) order by ts",
            [7, 7, 7, 7, None, 8, 8, None])

        tdSql.execute("create table tr_caly(ts timestamp, v int)")
        year_rows = [
            (utc_ms(2023, 1, 10), 5),
            (utc_ms(2024, 1, 7), None),
            (utc_ms(2024, 1, 13), None),
        ]
        insert_rows("tr_caly", [r[0] for r in year_rows], [r[1] for r in year_rows])
        expect_second_col(
            "select ts, v_p from table(time_align(table tr_caly, 'v:PREV(1y):_p')) order by ts",
            [5, 5, None])

        # Virtual table input: the align stage lives in the virtual table scan.
        tdSql.execute("create vtable tr_vt(ts timestamp, v int from tr_prev.v)")
        expect_second_col(
            "select ts, v_p from table(time_align(table tr_vt, 'v:PREV(2s):_p')) order by ts",
            [1, 1, 1, None, 9, 9, None])
        tdSql.execute("create vtable tr_vcal(ts timestamp, v int from tr_cal.v)")
        expect_second_col(
            "select ts, v_p from table(time_align(table tr_vcal, 'v:PREV(1q):_p')) order by ts",
            [7, 7, 7, 7, None, 8, 8, None])

        # Descending output keeps the scan ascending: the Sort stays above the
        # align stage, so the values equal the ascending result and only the
        # row order changes.
        expect_second_col(
            "select ts, v_p from table(time_align(table tr_prev, 'v:PREV(2s):_p')) order by ts desc",
            [None, 9, 9, None, 1, 1, 1])
        expect_second_col(
            "select ts, v_p from table(time_align(table tr_prev, 'v:PREV(2s):_p')) "
            "order by ts desc limit 3",
            [None, 9, 9])
        expect_second_col(
            "select ts, v_p from table(time_align(table tr_vt, 'v:PREV(2s):_p')) order by ts desc",
            [None, 9, 9, None, 1, 1, 1])
        expect_second_col(
            "select ts, v_p from table(time_align(table tr_vt, 'v:PREV(2s):_p')) "
            "order by ts desc limit 3",
            [None, 9, 9])
        # DESC is pushed into the scan (no Sort above the align stage).
        for sql in (
            "explain select ts, v_p from table(time_align(table tr_prev, 'v:PREV(2s):_p')) "
            "order by ts desc",
            "explain select ts, v_p from table(time_align(table tr_vt, 'v:PREV(2s):_p')) "
            "order by ts desc",
        ):
            tdSql.query(sql)
            plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
            assert "Sort" not in plan, plan
        # Virtual super table: the range stays per child, DESC only reorders.
        expect_second_col(
            "select ts, voltage_p from table(time_align(table vst_disjoint, 'voltage:PREV(0s):_p')) "
            "order by ts",
            [10, None, 20, 30])
        expect_second_col(
            "select ts, voltage_p from table(time_align(table vst_disjoint, 'voltage:PREV(0s):_p')) "
            "order by ts desc",
            [30, 20, None, 10])

        # View input ignores the time range and interpolates in view output
        # row order.  The timestamp does not have to be part of the output.
        tdSql.execute("create view tr_vw as select ts, v from tr_prev")
        tdSql.query("select v_p from table(time_align(table tr_vw, 'v:PREV(2s):_p'))")
        tdSql.checkRows(7)
        for i, value in enumerate([1, 1, 1, 1, 9, 9, 9]):
            tdSql.checkData(i, 0, value)
        expect_second_col(
            "select ts, v_p from table(time_align(table tr_vw, TRUE, 'v:PREV(2s):_p')) order by ts",
            [1, 1, 1, 1, 9, 9, 9])

        # A view without a TIMESTAMP data column also accepts the range
        # syntax; the range is ignored.
        tdSql.execute("create view tr_vnt as select cast(ts as bigint) as t, v from tr_prev")
        tdSql.query("select v_p from table(time_align(table tr_vnt, 'v:PREV(2s):_p'))")
        tdSql.checkRows(7)
        for i, value in enumerate([1, 1, 1, 1, 9, 9, 9]):
            tdSql.checkData(i, 0, value)

        # Invalid duration literals are method/parameter errors.
        for method in (
            "v:PREV(8):_p",
            "v:PREV(-8s):_p",
            "v:PREV(1.5s):_p",
            "v:PREV(8x):_p",
            "v:PREV():_p",
            "v:PREV(8s,1h):_p",
            "v:PREV((8s)):_p",
            "v:PREV(8s:_p",
            "v:PREV(8s)):_p",
            "v:(8s):_p",
            "v:PREV(d):_p",
        ):
            tdSql.error(
                f"select * from table(time_align(table tr_prev, '{method}'))",
                PAR_NOT_ALLOWED_FUNC)
