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
"""Comprehensive layered performance benchmark.

Compares per-layer overhead across all reference topologies:
  1. vtable ref normal-table (col-ref chain, L0-L8)
  2. vtable ref vtable (col-ref chain, L0-L8)
  3. vstable ref normal-stable (stable chain with literal tags, L0-L4)
  4. vstable ref vstable (stable chain with literal tags, L0-L4)
  5. vstable ref vstable + tag-ref (tag propagation through layers, L0-L4)
  6. Cross-topology comparison at same depth

All results written to /tmp/layered_perf_report.txt for analysis.
"""

import time
import statistics
from new_test_framework.utils import tdLog, tdSql

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_TS = 1700000000000
REPEATS = 5
REPORT_FILE = "/tmp/layered_perf_report.txt"

# Topology parameters
VTABLE_DEPTHS = [0, 1, 2, 4, 8]
VSTABLE_DEPTHS = [0, 1, 2, 3, 4]
CHILDREN = 20
ROWS_PER_CHILD = 1000
DATA_COLS = 5
TAG_COLS = 3

QUERIES = [
    ("SELECT *",   "SELECT * FROM {tbl}"),
    ("COUNT(*)",   "SELECT COUNT(*) FROM {tbl}"),
    ("SUM+AVG",    "SELECT SUM(c0), AVG(c1) FROM {tbl}"),
    ("filter",     "SELECT * FROM {tbl} WHERE c0 > 500"),
    ("LAST",       "SELECT LAST(c0) FROM {tbl}"),
]

STABLE_QUERIES = [
    ("SELECT *",    "SELECT * FROM {stb}"),
    ("COUNT(*)",    "SELECT COUNT(*) FROM {stb}"),
    ("SUM+AVG",     "SELECT SUM(c0), AVG(c1) FROM {stb}"),
    ("tag filter",  "SELECT * FROM {stb} WHERE t0 = 0"),
    ("GROUP BY",    "SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _bench(sql, repeats=REPEATS):
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        tdSql.query(sql)
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000.0)
    return times


def _median(lst):
    return statistics.median(lst)


_all_results = []
_report_lines = []


def _emit(line=""):
    _report_lines.append(line)
    tdLog.info(line)


def _result(topology, depth, query, median_ms, base_ms=None, prev_ms=None):
    vs_base = f"{median_ms / base_ms * 100:.0f}%" if base_ms and base_ms > 0 else "-"
    vs_prev = f"+{(median_ms - prev_ms) / prev_ms * 100:.0f}%" if prev_ms and prev_ms > 0 else "-"
    _all_results.append({
        "topology": topology, "depth": depth, "query": query,
        "ms": median_ms, "vs_base": vs_base, "vs_prev": vs_prev,
    })
    _emit(f"  {topology:<28} L{depth:<4} {query:<14} {median_ms:>10.2f} {vs_base:>10} {vs_prev:>10}")


def _section(title):
    _emit("")
    _emit(f"=== {title} ===")
    _emit(f"  {'Topology':<28} {'Depth':<5} {'Query':<14} {'Median(ms)':>10} {'vs L0':>10} {'vs Prev':>10}")
    _emit("  " + "-" * 90)


def _insert_rows(table, n_rows, n_cols=DATA_COLS, batch=1000):
    for start in range(0, n_rows, batch):
        end = min(start + batch, n_rows)
        vals = []
        for i in range(start, end):
            cols = ", ".join(str(i + j * 100) for j in range(n_cols))
            vals.append(f"({BASE_TS + i}, {cols})")
        tdSql.execute(f"INSERT INTO {table} VALUES " + ", ".join(vals))


def _flush():
    with open(REPORT_FILE, "w") as f:
        f.write("\n".join(_report_lines) + "\n")


# ===================================================================
# Test class
# ===================================================================
class TestVtableLayeredPerf:

    def setup_class(cls):
        """Build all layered topologies for comparison."""
        tdLog.info("=== Layered perf benchmark setup ===")

        _report_lines.clear()
        _all_results.clear()
        _emit("LAYERED VIRTUAL TABLE PERFORMANCE BENCHMARK")
        _emit("=" * 90)

        tdSql.execute('ALTER ALL DNODES "debugFlag 131";')
        tdSql.execute('ALTER LOCAL "debugFlag 131";')

        for db in ["lp_vt_norm", "lp_vt_vt", "lp_vstb_norm",
                    "lp_vstb_vstb", "lp_vstb_tagref", "lp_compare"]:
            tdSql.execute(f"DROP DATABASE IF EXISTS {db};")

        col_defs = ", ".join(f"c{i} INT" for i in range(DATA_COLS))
        tag_defs = ", ".join(f"t{i} INT" for i in range(TAG_COLS))

        # ===========================================================
        # Topology 1: vtable ref normal-table (chain L0-L8)
        # Each layer refs the source normal table directly
        # ===========================================================
        tdSql.execute("CREATE DATABASE lp_vt_norm VGROUPS 2;")
        tdSql.execute("USE lp_vt_norm;")

        tdSql.execute(f"CREATE TABLE src (ts TIMESTAMP, {col_defs});")
        _insert_rows("src", ROWS_PER_CHILD * CHILDREN)

        cls._vt_norm = {0: "src"}
        prev = "src"
        for d in range(1, max(VTABLE_DEPTHS) + 1):
            tbl = f"vt_l{d}"
            refs = ", ".join(f"c{j} INT FROM {prev}.c{j}" for j in range(DATA_COLS))
            tdSql.execute(f"CREATE VTABLE {tbl} (ts TIMESTAMP, {refs});")
            prev = tbl
            if d in VTABLE_DEPTHS:
                cls._vt_norm[d] = tbl
        tdLog.info("  topo 1 (vt ref norm) done")

        # ===========================================================
        # Topology 2: vtable ref vtable (chain L0-L8)
        # L1 refs source, L2+ refs previous vtable
        # ===========================================================
        tdSql.execute("CREATE DATABASE lp_vt_vt VGROUPS 2;")
        tdSql.execute("USE lp_vt_vt;")

        tdSql.execute(f"CREATE TABLE src (ts TIMESTAMP, {col_defs});")
        _insert_rows("src", ROWS_PER_CHILD * CHILDREN)

        cls._vt_vt = {0: "src"}
        prev = "src"
        for d in range(1, max(VTABLE_DEPTHS) + 1):
            tbl = f"vt_l{d}"
            refs = ", ".join(f"c{j} INT FROM {prev}.c{j}" for j in range(DATA_COLS))
            tdSql.execute(f"CREATE VTABLE {tbl} (ts TIMESTAMP, {refs});")
            prev = tbl
            if d in VTABLE_DEPTHS:
                cls._vt_vt[d] = tbl
        tdLog.info("  topo 2 (vt ref vt) done")

        # ===========================================================
        # Topology 3: vstable ref normal stable (L0-L4)
        # Virtual stables with children that ref normal child tables
        # ===========================================================
        tdSql.execute("CREATE DATABASE lp_vstb_norm VGROUPS 2;")
        tdSql.execute("USE lp_vstb_norm;")

        tdSql.execute(f"CREATE STABLE src_stb (ts TIMESTAMP, {col_defs}) TAGS ({tag_defs});")
        for c in range(CHILDREN):
            tag_vals = ", ".join(str(c * 10 + t) for t in range(TAG_COLS))
            tdSql.execute(f"CREATE TABLE src_c{c} USING src_stb TAGS ({tag_vals});")
            _insert_rows(f"src_c{c}", ROWS_PER_CHILD)

        cls._vstb_norm = {0: "src_stb"}
        prev_stb_name = "src_stb"
        for d in range(1, max(VSTABLE_DEPTHS) + 1):
            vstb = f"vstb_l{d}"
            tdSql.execute(f"CREATE STABLE {vstb} (ts TIMESTAMP, {col_defs}) "
                          f"TAGS ({tag_defs}) VIRTUAL 1;")
            for c in range(CHILDREN):
                prev_child = f"src_c{c}" if d == 1 else f"vc_l{d-1}_c{c}"
                vt = f"vc_l{d}_c{c}"
                col_refs = ", ".join(f"c{j} FROM {prev_child}.c{j}" for j in range(DATA_COLS))
                tag_vals = ", ".join(str(c * 10 + t + d * 100) for t in range(TAG_COLS))
                tdSql.execute(f"CREATE VTABLE {vt} ({col_refs}) "
                              f"USING {vstb} TAGS ({tag_vals});")
            prev_stb_name = vstb
            if d in VSTABLE_DEPTHS:
                cls._vstb_norm[d] = vstb
        tdLog.info("  topo 3 (vstb ref norm) done")

        # ===========================================================
        # Topology 4: vstable ref vstable (L0-L4, literal tags)
        # Same as topo 3 but emphasizing vstable->vstable chain
        # (already covered above since L2+ refs prev vstable children)
        # Store separately for clarity in reporting
        # ===========================================================
        cls._vstb_vstb = cls._vstb_norm  # same topology, L2+ is vstable-ref-vstable
        tdLog.info("  topo 4 (vstb ref vstb) = topo 3 (L2+ are vstb-ref-vstb)")

        # ===========================================================
        # Topology 5: vstable ref vstable + tag-ref (L0-L4)
        # Each layer propagates tags via TAG_REF from source
        # ===========================================================
        tdSql.execute("CREATE DATABASE lp_vstb_tagref VGROUPS 2;")
        tdSql.execute("USE lp_vstb_tagref;")

        # Source stable with tags (provides tag values for tag-ref)
        tdSql.execute(f"CREATE STABLE src_stb (ts TIMESTAMP, {col_defs}) TAGS ({tag_defs});")
        for c in range(CHILDREN):
            tag_vals = ", ".join(str(c * 10 + t) for t in range(TAG_COLS))
            tdSql.execute(f"CREATE TABLE src_c{c} USING src_stb TAGS ({tag_vals});")
            _insert_rows(f"src_c{c}", ROWS_PER_CHILD)

        # Tag source stable (provides tag columns to reference)
        tdSql.execute(f"CREATE STABLE tag_src (ts TIMESTAMP, dummy INT) TAGS ({tag_defs});")
        for c in range(CHILDREN):
            tag_vals = ", ".join(str(c * 10 + t) for t in range(TAG_COLS))
            tdSql.execute(f"CREATE TABLE tag_c{c} USING tag_src TAGS ({tag_vals});")
            tdSql.execute(f"INSERT INTO tag_c{c} VALUES ({BASE_TS}, 0);")

        cls._vstb_tagref = {0: "src_stb"}
        for d in range(1, max(VSTABLE_DEPTHS) + 1):
            vstb = f"vstb_tr_l{d}"
            tdSql.execute(f"CREATE STABLE {vstb} (ts TIMESTAMP, {col_defs}) "
                          f"TAGS ({tag_defs}) VIRTUAL 1;")
            for c in range(CHILDREN):
                prev_child = f"src_c{c}" if d == 1 else f"vc_tr_l{d-1}_c{c}"
                vt = f"vc_tr_l{d}_c{c}"
                col_refs = ", ".join(f"c{j} FROM {prev_child}.c{j}" for j in range(DATA_COLS))
                tag_refs = ", ".join(f"t{t} FROM tag_c{c}.t{t}" for t in range(TAG_COLS))
                tdSql.execute(f"CREATE VTABLE {vt} ({col_refs}) "
                              f"USING {vstb} TAGS ({tag_refs});")
            if d in VSTABLE_DEPTHS:
                cls._vstb_tagref[d] = vstb
        tdLog.info("  topo 5 (vstb ref vstb + tag-ref) done")

        # ===========================================================
        # Topology 6: Cross-topology comparison at L1 and L2
        # All types in one DB for fair comparison
        # ===========================================================
        tdSql.execute("CREATE DATABASE lp_compare VGROUPS 2;")
        tdSql.execute("USE lp_compare;")

        # Normal table (baseline)
        tdSql.execute(f"CREATE TABLE norm_tbl (ts TIMESTAMP, {col_defs});")
        _insert_rows("norm_tbl", ROWS_PER_CHILD * CHILDREN)

        # Normal stable + children (baseline for stable queries)
        tdSql.execute(f"CREATE STABLE norm_stb (ts TIMESTAMP, {col_defs}) TAGS ({tag_defs});")
        for c in range(CHILDREN):
            tag_vals = ", ".join(str(c * 10 + t) for t in range(TAG_COLS))
            tdSql.execute(f"CREATE TABLE norm_c{c} USING norm_stb TAGS ({tag_vals});")
            _insert_rows(f"norm_c{c}", ROWS_PER_CHILD)

        # Tag source for tag-refs
        tdSql.execute(f"CREATE STABLE cmp_tag_src (ts TIMESTAMP, dummy INT) TAGS ({tag_defs});")
        for c in range(CHILDREN):
            tag_vals = ", ".join(str(c * 10 + t) for t in range(TAG_COLS))
            tdSql.execute(f"CREATE TABLE cmp_tag_c{c} USING cmp_tag_src TAGS ({tag_vals});")
            tdSql.execute(f"INSERT INTO cmp_tag_c{c} VALUES ({BASE_TS}, 0);")

        # vtable L1 ref normal
        refs = ", ".join(f"c{j} INT FROM norm_tbl.c{j}" for j in range(DATA_COLS))
        tdSql.execute(f"CREATE VTABLE cmp_vt_l1 (ts TIMESTAMP, {refs});")
        # vtable L2 ref vtable
        refs = ", ".join(f"c{j} INT FROM cmp_vt_l1.c{j}" for j in range(DATA_COLS))
        tdSql.execute(f"CREATE VTABLE cmp_vt_l2 (ts TIMESTAMP, {refs});")

        # vstable L1 ref normal (literal tags)
        tdSql.execute(f"CREATE STABLE cmp_vstb_lit (ts TIMESTAMP, {col_defs}) "
                      f"TAGS ({tag_defs}) VIRTUAL 1;")
        for c in range(CHILDREN):
            tag_vals = ", ".join(str(c * 10 + t) for t in range(TAG_COLS))
            tdSql.execute(f"CREATE VTABLE cmp_vc_lit_c{c} ("
                          + ", ".join(f"c{j} FROM norm_c{c}.c{j}" for j in range(DATA_COLS))
                          + f") USING cmp_vstb_lit TAGS ({tag_vals});")

        # vstable L1 ref normal (tag-ref)
        tdSql.execute(f"CREATE STABLE cmp_vstb_tref (ts TIMESTAMP, {col_defs}) "
                      f"TAGS ({tag_defs}) VIRTUAL 1;")
        for c in range(CHILDREN):
            col_refs = ", ".join(f"c{j} FROM norm_c{c}.c{j}" for j in range(DATA_COLS))
            tag_refs = ", ".join(f"t{t} FROM cmp_tag_c{c}.t{t}" for t in range(TAG_COLS))
            tdSql.execute(f"CREATE VTABLE cmp_vc_tref_c{c} ({col_refs}) "
                          f"USING cmp_vstb_tref TAGS ({tag_refs});")

        # vstable L2 ref vstable (literal tags)
        tdSql.execute(f"CREATE STABLE cmp_vstb_lit2 (ts TIMESTAMP, {col_defs}) "
                      f"TAGS ({tag_defs}) VIRTUAL 1;")
        for c in range(CHILDREN):
            tag_vals = ", ".join(str(c * 10 + t + 200) for t in range(TAG_COLS))
            tdSql.execute(f"CREATE VTABLE cmp_vc_lit2_c{c} ("
                          + ", ".join(f"c{j} FROM cmp_vc_lit_c{c}.c{j}" for j in range(DATA_COLS))
                          + f") USING cmp_vstb_lit2 TAGS ({tag_vals});")

        # vstable L2 ref vstable (tag-ref)
        tdSql.execute(f"CREATE STABLE cmp_vstb_tref2 (ts TIMESTAMP, {col_defs}) "
                      f"TAGS ({tag_defs}) VIRTUAL 1;")
        for c in range(CHILDREN):
            col_refs = ", ".join(f"c{j} FROM cmp_vc_tref_c{c}.c{j}" for j in range(DATA_COLS))
            tag_refs = ", ".join(f"t{t} FROM cmp_tag_c{c}.t{t}" for t in range(TAG_COLS))
            tdSql.execute(f"CREATE VTABLE cmp_vc_tref2_c{c} ({col_refs}) "
                          f"USING cmp_vstb_tref2 TAGS ({tag_refs});")

        cls._cmp = {
            "norm_tbl": "norm_tbl",
            "norm_stb": "norm_stb",
            "vt_l1": "cmp_vt_l1",
            "vt_l2": "cmp_vt_l2",
            "vstb_lit_l1": "cmp_vstb_lit",
            "vstb_tref_l1": "cmp_vstb_tref",
            "vstb_lit_l2": "cmp_vstb_lit2",
            "vstb_tref_l2": "cmp_vstb_tref2",
        }
        tdLog.info("  topo 6 (cross-topology comparison) done")
        tdLog.info("=== Layered perf setup complete ===")

    # ---------------------------------------------------------------
    # Test 1: vtable ref normal-table depth chain
    # ---------------------------------------------------------------
    def test_01_vt_ref_norm_depth(self):
        """Vtable referencing normal table: per-layer overhead L0-L8

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, benchmark, layered

        Jira: None

        History:
            - 2026-04-18 Created
        """
        tdSql.execute("USE lp_vt_norm;")
        _section("vtable ref normal-table: depth chain")

        for qname, qtpl in QUERIES:
            base_ms = None
            prev_ms = None
            for d in VTABLE_DEPTHS:
                tbl = self._vt_norm[d]
                ms = _median(_bench(qtpl.format(tbl=tbl)))
                if base_ms is None:
                    base_ms = ms
                _result("vt-ref-norm", d, qname, ms, base_ms, prev_ms)
                prev_ms = ms
        _flush()

    # ---------------------------------------------------------------
    # Test 2: vtable ref vtable depth chain
    # ---------------------------------------------------------------
    def test_02_vt_ref_vt_depth(self):
        """Vtable referencing vtable: per-layer overhead L0-L8

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, benchmark, layered

        Jira: None

        History:
            - 2026-04-18 Created
        """
        tdSql.execute("USE lp_vt_vt;")
        _section("vtable ref vtable: depth chain")

        for qname, qtpl in QUERIES:
            base_ms = None
            prev_ms = None
            for d in VTABLE_DEPTHS:
                tbl = self._vt_vt[d]
                ms = _median(_bench(qtpl.format(tbl=tbl)))
                if base_ms is None:
                    base_ms = ms
                _result("vt-ref-vt", d, qname, ms, base_ms, prev_ms)
                prev_ms = ms
        _flush()

    # ---------------------------------------------------------------
    # Test 3: vstable ref normal stable depth chain
    # ---------------------------------------------------------------
    def test_03_vstb_ref_norm_depth(self):
        """Vstable referencing normal stable: per-layer overhead L0-L4

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, benchmark, layered

        Jira: None

        History:
            - 2026-04-18 Created
        """
        tdSql.execute("USE lp_vstb_norm;")
        _section("vstable ref normal-stable: depth chain (literal tags)")

        for qname, qtpl in STABLE_QUERIES:
            base_ms = None
            prev_ms = None
            for d in VSTABLE_DEPTHS:
                stb = self._vstb_norm[d]
                ms = _median(_bench(qtpl.format(stb=stb)))
                if base_ms is None:
                    base_ms = ms
                _result("vstb-ref-norm(lit)", d, qname, ms, base_ms, prev_ms)
                prev_ms = ms
        _flush()

    # ---------------------------------------------------------------
    # Test 4: vstable ref vstable + tag-ref depth chain
    # ---------------------------------------------------------------
    def test_04_vstb_tagref_depth(self):
        """Vstable referencing vstable with tag-ref: per-layer overhead L0-L4

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, benchmark, layered

        Jira: None

        History:
            - 2026-04-18 Created
        """
        tdSql.execute("USE lp_vstb_tagref;")
        _section("vstable ref vstable: depth chain (tag-ref)")

        for qname, qtpl in STABLE_QUERIES:
            base_ms = None
            prev_ms = None
            for d in VSTABLE_DEPTHS:
                stb = self._vstb_tagref[d]
                ms = _median(_bench(qtpl.format(stb=stb)))
                if base_ms is None:
                    base_ms = ms
                _result("vstb-ref-vstb(tref)", d, qname, ms, base_ms, prev_ms)
                prev_ms = ms
        _flush()

    # ---------------------------------------------------------------
    # Test 5: Cross-topology comparison at same depth
    # ---------------------------------------------------------------
    def test_05_cross_topology_compare(self):
        """Cross-topology comparison: all types at L1 and L2

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, benchmark, layered

        Jira: None

        History:
            - 2026-04-18 Created
        """
        tdSql.execute("USE lp_compare;")

        # --- Single-table queries (vtable vs normal) ---
        _section("Cross-topology: single-table queries")

        single_types = [
            ("normal-table",    "norm_tbl",  0),
            ("vt-ref-norm L1",  "vt_l1",     1),
            ("vt-ref-vt L2",    "vt_l2",     2),
        ]
        for qname, qtpl in QUERIES:
            base_ms = None
            for label, key, depth in single_types:
                tbl = self._cmp[key]
                ms = _median(_bench(qtpl.format(tbl=tbl)))
                if base_ms is None:
                    base_ms = ms
                _result(label, depth, qname, ms, base_ms)

        # --- Stable queries (vstable variants) ---
        _section("Cross-topology: stable queries")

        stable_types = [
            ("normal-stable",         "norm_stb",       0),
            ("vstb-lit L1",           "vstb_lit_l1",    1),
            ("vstb-tagref L1",        "vstb_tref_l1",   1),
            ("vstb-lit L2",           "vstb_lit_l2",    2),
            ("vstb-tagref L2",        "vstb_tref_l2",   2),
        ]
        for qname, qtpl in STABLE_QUERIES:
            base_ms = None
            for label, key, depth in stable_types:
                stb = self._cmp[key]
                ms = _median(_bench(qtpl.format(stb=stb)))
                if base_ms is None:
                    base_ms = ms
                _result(label, depth, qname, ms, base_ms)

        _flush()

    # ---------------------------------------------------------------
    # Test 6: Per-layer marginal cost
    # ---------------------------------------------------------------
    def test_06_marginal_cost(self):
        """Per-layer marginal cost: how much does each additional layer add?

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, benchmark, layered

        Jira: None

        History:
            - 2026-04-18 Created
        """
        _section("Per-layer marginal cost (COUNT(*) query)")

        # vtable chain
        tdSql.execute("USE lp_vt_vt;")
        vt_times = {}
        for d in VTABLE_DEPTHS:
            tbl = self._vt_vt[d]
            vt_times[d] = _median(_bench(f"SELECT COUNT(*) FROM {tbl}"))

        prev = None
        for d in VTABLE_DEPTHS:
            ms = vt_times[d]
            delta = ms - prev if prev is not None else 0
            _emit(f"  vt-ref-vt chain    L{d:<4} COUNT(*)       {ms:>10.2f}ms   delta: {delta:>+8.2f}ms")
            prev = ms

        # vstable chain (literal)
        tdSql.execute("USE lp_vstb_norm;")
        vstb_times = {}
        for d in VSTABLE_DEPTHS:
            stb = self._vstb_norm[d]
            vstb_times[d] = _median(_bench(f"SELECT COUNT(*) FROM {stb}"))

        _emit("")
        prev = None
        for d in VSTABLE_DEPTHS:
            ms = vstb_times[d]
            delta = ms - prev if prev is not None else 0
            _emit(f"  vstb-lit chain     L{d:<4} COUNT(*)       {ms:>10.2f}ms   delta: {delta:>+8.2f}ms")
            prev = ms

        # vstable chain (tag-ref)
        tdSql.execute("USE lp_vstb_tagref;")
        vstb_tr_times = {}
        for d in VSTABLE_DEPTHS:
            stb = self._vstb_tagref[d]
            vstb_tr_times[d] = _median(_bench(f"SELECT COUNT(*) FROM {stb}"))

        _emit("")
        prev = None
        for d in VSTABLE_DEPTHS:
            ms = vstb_tr_times[d]
            delta = ms - prev if prev is not None else 0
            _emit(f"  vstb-tagref chain  L{d:<4} COUNT(*)       {ms:>10.2f}ms   delta: {delta:>+8.2f}ms")
            prev = ms

        _flush()

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------
    def test_zz_summary(self):
        """Write final report summary

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, benchmark, layered

        Jira: None

        History:
            - 2026-04-18 Created
        """
        _emit("")
        _emit("=" * 90)
        _emit("  LAYERED BENCHMARK COMPLETE")
        _emit(f"  Total measurements: {len(_all_results)}")
        _emit("=" * 90)
        _flush()
        tdLog.info(f"Full report written to {REPORT_FILE}")
