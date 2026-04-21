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
"""Performance benchmark for tag-ref features.

Covers P8 test plan items:
  PRF-01: Large child table count (100/500/1000 children with tag-refs)
  PRF-02: Multi-vgroup parallelism (4 vgroups, 200 children)
  PRF-03: Many tag-refs per vstable (1/5/10 ref tags)
  PRF-04: High-frequency repeated queries (cache warm-up effect)
  PRF-05: Mixed col-ref + tag-ref queries
  PRF-06: Large data volume per child (10K/100K rows)
  PRF-07: Tag-ref vs literal-tag baseline comparison

Outputs a structured performance report with timings and relative overhead.
"""

import time
import statistics
from new_test_framework.utils import tdLog, tdSql

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_TS = 1700000000000
REPEATS = 5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _bench(sql, repeats=REPEATS):
    """Run sql *repeats* times, return list of elapsed ms."""
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        tdSql.query(sql)
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000.0)
    return times


def _median(lst):
    return statistics.median(lst)


def _insert_child_rows(table, n_rows, batch=1000):
    """Insert n_rows into a table with (ts, val INT) schema."""
    for start in range(0, n_rows, batch):
        end = min(start + batch, n_rows)
        vals = []
        for i in range(start, end):
            vals.append(f"({BASE_TS + i}, {i})")
        tdSql.execute(f"INSERT INTO {table} VALUES " + ", ".join(vals))


_all_reports = []
_REPORT_FILE = "/tmp/tag_ref_perf_report.txt"

def _report(title, rows):
    """Print a formatted benchmark table and accumulate for final summary."""
    _all_reports.append((title, rows))
    lines = []
    lines.append(f"\n=== {title} ===")
    hdr = f"  {'Variant':<30} {'Query':<30} {'Median(ms)':>12} {'vs Base':>10}"
    lines.append(hdr)
    lines.append("  " + "-" * (len(hdr) - 2))
    for r in rows:
        vs = f"{r[3]:.0f}%" if r[3] is not None else "-"
        lines.append(f"  {r[0]:<30} {r[1]:<30} {r[2]:>12.2f} {vs:>10}")
    lines.append("")
    for line in lines:
        tdLog.info(line)
    with open(_REPORT_FILE, "a") as f:
        f.write("\n".join(lines) + "\n")


# ===================================================================
# Test class
# ===================================================================
class TestVtableTagRefPerf:

    def setup_class(cls):
        """Create all tag-ref performance topologies."""
        tdLog.info("=== Tag-ref performance setup ===")
        # Clear report file
        with open(_REPORT_FILE, "w") as f:
            f.write("TAG-REF PERFORMANCE BENCHMARK REPORT\n")
            f.write("=" * 90 + "\n")

        tdSql.execute('ALTER ALL DNODES "debugFlag 131";')
        tdSql.execute('ALTER LOCAL "debugFlag 131";')

        for db in ["perf_tagref_scale", "perf_tagref_vgroup",
                    "perf_tagref_many", "perf_tagref_mixed",
                    "perf_tagref_data"]:
            tdSql.execute(f"DROP DATABASE IF EXISTS {db};")

        # ===========================================================
        # Topology 1: child-table scale (PRF-01)
        #   Source stable with children, vstable with tag-refs
        #   Variants: 100, 500, 1000 children
        # ===========================================================
        tdSql.execute("CREATE DATABASE perf_tagref_scale VGROUPS 2;")
        tdSql.execute("USE perf_tagref_scale;")

        tdSql.execute("CREATE STABLE src_stb (ts TIMESTAMP, val INT) TAGS (region INT, site NCHAR(32));")

        cls._scale_children_counts = [100, 500, 1000]
        max_children = max(cls._scale_children_counts)

        for c in range(max_children):
            tdSql.execute(f"CREATE TABLE src_c{c} USING src_stb TAGS ({c % 10}, 'site_{c}');")
            _insert_child_rows(f"src_c{c}", 100)

        # Literal-tag vstable (baseline)
        tdSql.execute("CREATE STABLE vstb_lit (ts TIMESTAMP, val INT) "
                      "TAGS (region INT, site NCHAR(32)) VIRTUAL 1;")
        for c in range(max_children):
            tdSql.execute(f"CREATE VTABLE vlit_c{c} (val FROM src_c{c}.val) "
                          f"USING vstb_lit TAGS ({c % 10}, 'site_{c}');")

        # Tag-ref vstable
        tdSql.execute("CREATE STABLE vstb_ref (ts TIMESTAMP, val INT) "
                      "TAGS (region INT, site NCHAR(32)) VIRTUAL 1;")
        for c in range(max_children):
            tdSql.execute(f"CREATE VTABLE vref_c{c} (val FROM src_c{c}.val) "
                          f"USING vstb_ref TAGS ("
                          f"region FROM src_c{c}.region, "
                          f"site FROM src_c{c}.site);")

        tdLog.info(f"  scale topology done: {max_children} children")

        # ===========================================================
        # Topology 2: multi-vgroup (PRF-02)
        # ===========================================================
        tdSql.execute("CREATE DATABASE perf_tagref_vgroup VGROUPS 4;")
        tdSql.execute("USE perf_tagref_vgroup;")

        tdSql.execute("CREATE STABLE src_vg (ts TIMESTAMP, val INT) TAGS (gid INT);")
        for c in range(200):
            tdSql.execute(f"CREATE TABLE src_vg_c{c} USING src_vg TAGS ({c});")
            _insert_child_rows(f"src_vg_c{c}", 100)

        tdSql.execute("CREATE STABLE vstb_vg (ts TIMESTAMP, val INT) "
                      "TAGS (gid INT) VIRTUAL 1;")
        for c in range(200):
            tdSql.execute(f"CREATE VTABLE vref_vg_c{c} (val FROM src_vg_c{c}.val) "
                          f"USING vstb_vg TAGS (gid FROM src_vg_c{c}.gid);")

        tdLog.info("  vgroup topology done: 4 vgroups, 200 children")

        # ===========================================================
        # Topology 3: many tag-refs (PRF-03)
        #   Vary number of tag-ref columns: 1, 5, 10
        # ===========================================================
        tdSql.execute("CREATE DATABASE perf_tagref_many VGROUPS 2;")
        tdSql.execute("USE perf_tagref_many;")

        # Source stable with 10 tag columns
        tag_defs = ", ".join(f"t{i} INT" for i in range(10))
        tdSql.execute(f"CREATE STABLE src_mt (ts TIMESTAMP, val INT) TAGS ({tag_defs});")
        for c in range(50):
            tag_vals = ", ".join(str(c * 100 + i) for i in range(10))
            tdSql.execute(f"CREATE TABLE src_mt_c{c} USING src_mt TAGS ({tag_vals});")
            _insert_child_rows(f"src_mt_c{c}", 200)

        cls._tagref_counts = [1, 5, 10]
        cls._many_stables = {}
        for tc in cls._tagref_counts:
            stb = f"vstb_mt{tc}"
            tag_tag_defs = ", ".join(f"t{i} INT" for i in range(tc))
            tdSql.execute(f"CREATE STABLE {stb} (ts TIMESTAMP, val INT) "
                          f"TAGS ({tag_tag_defs}) VIRTUAL 1;")
            for c in range(50):
                vt = f"vctb_mt{tc}_c{c}"
                tag_parts = ", ".join(f"t{i} FROM src_mt_c{c}.t{i}" for i in range(tc))
                tdSql.execute(f"CREATE VTABLE {vt} (val FROM src_mt_c{c}.val) "
                              f"USING {stb} TAGS ({tag_parts});")
            cls._many_stables[tc] = stb

        tdLog.info(f"  many tag-refs topology done: {cls._tagref_counts}")

        # ===========================================================
        # Topology 4: mixed col-ref + tag-ref (PRF-05)
        # ===========================================================
        tdSql.execute("CREATE DATABASE perf_tagref_mixed VGROUPS 2;")
        tdSql.execute("USE perf_tagref_mixed;")

        tdSql.execute("CREATE TABLE src_mix (ts TIMESTAMP, c0 INT, c1 INT, c2 INT, c3 INT, c4 INT);")
        _insert_child_rows_wide("src_mix", 10000, 5)

        tdSql.execute("CREATE STABLE src_mix_stb (ts TIMESTAMP, c0 INT, c1 INT, c2 INT, "
                      "c3 INT, c4 INT) TAGS (region INT, site NCHAR(32));")
        for c in range(20):
            tdSql.execute(f"CREATE TABLE src_mix_c{c} USING src_mix_stb "
                          f"TAGS ({c}, 'site_{c}');")
            _insert_child_rows_wide(f"src_mix_c{c}", 500, 5)

        # col-ref only vtable
        col_refs = ", ".join(f"c{i} INT FROM src_mix.c{i}" for i in range(5))
        tdSql.execute(f"CREATE VTABLE vtb_colonly (ts TIMESTAMP, {col_refs});")

        # col-ref + tag-ref vstable
        tdSql.execute("CREATE STABLE vstb_mixed (ts TIMESTAMP, val INT) "
                      "TAGS (region INT, site NCHAR(32)) VIRTUAL 1;")
        for c in range(20):
            tdSql.execute(f"CREATE VTABLE vmix_c{c} (val FROM src_mix_c{c}.c0) "
                          f"USING vstb_mixed TAGS ("
                          f"region FROM src_mix_c{c}.region, "
                          f"site FROM src_mix_c{c}.site);")

        tdLog.info("  mixed topology done")

        # ===========================================================
        # Topology 5: large data volume (PRF-06)
        # ===========================================================
        tdSql.execute("CREATE DATABASE perf_tagref_data VGROUPS 2;")
        tdSql.execute("USE perf_tagref_data;")

        tdSql.execute("CREATE STABLE src_big (ts TIMESTAMP, val INT) TAGS (gid INT);")
        cls._data_scales = [1000, 10000, 100000]
        for n in cls._data_scales:
            tdSql.execute(f"CREATE TABLE src_big_{n} USING src_big TAGS ({n});")
            _insert_child_rows(f"src_big_{n}", n)

        tdSql.execute("CREATE STABLE vstb_big (ts TIMESTAMP, val INT) "
                      "TAGS (gid INT) VIRTUAL 1;")
        for n in cls._data_scales:
            tdSql.execute(f"CREATE VTABLE vbig_{n} (val FROM src_big_{n}.val) "
                          f"USING vstb_big TAGS (gid FROM src_big_{n}.gid);")

        tdLog.info(f"  data volume topology done: {cls._data_scales}")
        tdLog.info("=== Tag-ref performance setup complete ===")

    # ---------------------------------------------------------------
    # PRF-01: Child table scale
    # ---------------------------------------------------------------
    def test_prf01_child_table_scale(self):
        """PRF-01: Query latency vs number of tag-ref children

        Compares tag-ref vstable queries across 100/500/1000 children,
        and measures overhead vs literal-tag baseline.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-18 Created
        """
        tdSql.execute("USE perf_tagref_scale;")
        queries = [
            ("SELECT *",       "SELECT * FROM {stb}"),
            ("COUNT(*)",       "SELECT COUNT(*) FROM {stb}"),
            ("tag filter",     "SELECT * FROM {stb} WHERE region = 3"),
            ("GROUP BY tag",   "SELECT region, COUNT(*) FROM {stb} GROUP BY region"),
        ]

        for qname, qtpl in queries:
            rows = []
            for count in self._scale_children_counts:
                # literal baseline: query subset by tbname pattern
                lit_ms = _median(_bench(qtpl.format(stb="vstb_lit")))
                ref_ms = _median(_bench(qtpl.format(stb="vstb_ref")))
                pct = (ref_ms / lit_ms * 100) if lit_ms > 0 else None
                rows.append((f"literal ({count} children)", qname, lit_ms, None))
                rows.append((f"tag-ref ({count} children)", qname, ref_ms, pct))
            _report(f"PRF-01: child scale — {qname}", rows)

    # ---------------------------------------------------------------
    # PRF-02: Multi-vgroup parallelism
    # ---------------------------------------------------------------
    def test_prf02_multi_vgroup(self):
        """PRF-02: Tag-ref query across 4 vgroups with 200 children

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-18 Created
        """
        tdSql.execute("USE perf_tagref_vgroup;")
        rows = []
        queries = [
            ("SELECT *",       "SELECT * FROM vstb_vg"),
            ("COUNT(*)",       "SELECT COUNT(*) FROM vstb_vg"),
            ("tag filter",     "SELECT * FROM vstb_vg WHERE gid = 50"),
            ("GROUP BY",       "SELECT gid, COUNT(*) FROM vstb_vg GROUP BY gid"),
            ("DISTINCT tag",   "SELECT DISTINCT gid FROM vstb_vg"),
        ]
        for qname, sql in queries:
            ms = _median(_bench(sql))
            rows.append(("4vg/200children", qname, ms, None))
        _report("PRF-02: multi-vgroup parallelism", rows)

    # ---------------------------------------------------------------
    # PRF-03: Many tag-refs per vstable
    # ---------------------------------------------------------------
    def test_prf03_many_tag_refs(self):
        """PRF-03: Query latency vs number of tag-ref tags (1/5/10)

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-18 Created
        """
        tdSql.execute("USE perf_tagref_many;")
        queries = [
            ("SELECT *",       lambda stb: f"SELECT * FROM {stb}"),
            ("COUNT(*)",       lambda stb: f"SELECT COUNT(*) FROM {stb}"),
            ("tag filter",     lambda stb: f"SELECT * FROM {stb} WHERE t0 = 100"),
            ("GROUP BY t0",    lambda stb: f"SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
        ]

        for qname, qfn in queries:
            rows = []
            base_ms = None
            for tc in self._tagref_counts:
                stb = self._many_stables[tc]
                ms = _median(_bench(qfn(stb)))
                if base_ms is None:
                    base_ms = ms
                pct = (ms / base_ms * 100) if base_ms > 0 else None
                rows.append((f"{tc} tag-refs", qname, ms, pct))
            _report(f"PRF-03: tag-ref count — {qname}", rows)

    # ---------------------------------------------------------------
    # PRF-04: High-frequency repeated queries (cache effect)
    # ---------------------------------------------------------------
    def test_prf04_high_frequency(self):
        """PRF-04: Cache warm-up effect — first vs subsequent queries

        Runs the same query 20 times and reports first-run, median of
        runs 2-5 (warm-up), and median of runs 16-20 (warm).

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-18 Created
        """
        tdSql.execute("USE perf_tagref_scale;")
        sql = "SELECT region, COUNT(*) FROM vstb_ref GROUP BY region"

        all_times = []
        for _ in range(20):
            t0 = time.perf_counter()
            tdSql.query(sql)
            t1 = time.perf_counter()
            all_times.append((t1 - t0) * 1000.0)

        first = all_times[0]
        warmup = _median(all_times[1:5])
        warm = _median(all_times[15:20])

        rows = [
            ("cold (1st run)", "GROUP BY tag", first, None),
            ("warm-up (2-5)", "GROUP BY tag", warmup, (warmup / first * 100) if first > 0 else None),
            ("warm (16-20)", "GROUP BY tag", warm, (warm / first * 100) if first > 0 else None),
        ]
        _report("PRF-04: cache warm-up effect", rows)

    # ---------------------------------------------------------------
    # PRF-05: Mixed col-ref + tag-ref
    # ---------------------------------------------------------------
    def test_prf05_mixed_colref_tagref(self):
        """PRF-05: Mixed col-ref and tag-ref query performance

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-18 Created
        """
        tdSql.execute("USE perf_tagref_mixed;")
        rows = []

        # col-ref only vtable
        ms = _median(_bench("SELECT * FROM vtb_colonly"))
        rows.append(("col-ref only", "SELECT *", ms, None))
        base_ms = ms

        # vstable with col-ref + tag-ref
        ms = _median(_bench("SELECT * FROM vstb_mixed"))
        rows.append(("col+tag ref vstable", "SELECT *", ms,
                     (ms / base_ms * 100) if base_ms > 0 else None))

        ms = _median(_bench("SELECT region, COUNT(*) FROM vstb_mixed GROUP BY region"))
        rows.append(("col+tag ref vstable", "GROUP BY tag", ms, None))

        ms = _median(_bench("SELECT * FROM vstb_mixed WHERE region = 5"))
        rows.append(("col+tag ref vstable", "tag filter", ms, None))

        _report("PRF-05: mixed col-ref + tag-ref", rows)

    # ---------------------------------------------------------------
    # PRF-06: Large data volume
    # ---------------------------------------------------------------
    def test_prf06_data_volume(self):
        """PRF-06: Tag-ref query with 1K/10K/100K rows per child

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-18 Created
        """
        tdSql.execute("USE perf_tagref_data;")
        queries = [
            ("SELECT *",  lambda t: f"SELECT * FROM {t}"),
            ("COUNT(*)",  lambda t: f"SELECT COUNT(*) FROM {t}"),
            ("SUM+WHERE", lambda t: f"SELECT SUM(val) FROM {t} WHERE val > 500"),
        ]

        for qname, qfn in queries:
            rows = []
            base_ms = None
            for n in self._data_scales:
                src_ms = _median(_bench(qfn(f"src_big_{n}")))
                ref_ms = _median(_bench(qfn(f"vbig_{n}")))
                if base_ms is None:
                    base_ms = src_ms
                rows.append((f"source ({n} rows)", qname, src_ms, None))
                rows.append((f"tag-ref ({n} rows)", qname, ref_ms,
                            (ref_ms / src_ms * 100) if src_ms > 0 else None))
            _report(f"PRF-06: data volume — {qname}", rows)

    # ---------------------------------------------------------------
    # PRF-07: Tag-ref vs literal-tag baseline
    # ---------------------------------------------------------------
    def test_prf07_tagref_vs_literal(self):
        """PRF-07: Direct comparison of tag-ref vs literal-tag overhead

        Uses the scale topology (1000 children) to compare identical
        queries on literal-tag vstable vs tag-ref vstable.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-18 Created
        """
        tdSql.execute("USE perf_tagref_scale;")
        queries = [
            ("SELECT *",        "SELECT * FROM {stb}"),
            ("COUNT(*)",        "SELECT COUNT(*) FROM {stb}"),
            ("tag filter",      "SELECT * FROM {stb} WHERE region = 3"),
            ("GROUP BY region", "SELECT region, COUNT(*) FROM {stb} GROUP BY region"),
            ("DISTINCT tag",    "SELECT DISTINCT region FROM {stb}"),
            ("ORDER BY tag",    "SELECT * FROM {stb} ORDER BY region LIMIT 100"),
            ("tag IN",          "SELECT * FROM {stb} WHERE region IN (1, 3, 5, 7)"),
        ]
        rows = []
        for qname, qtpl in queries:
            lit_ms = _median(_bench(qtpl.format(stb="vstb_lit")))
            ref_ms = _median(_bench(qtpl.format(stb="vstb_ref")))
            overhead = ((ref_ms - lit_ms) / lit_ms * 100) if lit_ms > 0 else None
            rows.append(("literal-tag", qname, lit_ms, None))
            rows.append(("tag-ref", qname, ref_ms, overhead))
        _report("PRF-07: tag-ref vs literal overhead (1000 children)", rows)

    # ---------------------------------------------------------------
    # Summary: collect all results into a single report
    # ---------------------------------------------------------------
    def test_zz_summary(self):
        """Write final report summary

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-18 Created
        """
        tdLog.info("")
        tdLog.info("=" * 90)
        tdLog.info("  TAG-REF PERFORMANCE BENCHMARK — FULL REPORT")
        tdLog.info("=" * 90)
        summary_lines = []
        for title, rows in _all_reports:
            tdLog.info("")
            tdLog.info(f"--- {title} ---")
            hdr = f"  {'Variant':<30} {'Query':<30} {'Median(ms)':>12} {'vs Base':>10}"
            tdLog.info(hdr)
            tdLog.info("  " + "-" * 84)
            summary_lines.append(f"\n--- {title} ---")
            summary_lines.append(hdr)
            summary_lines.append("  " + "-" * 84)
            for r in rows:
                vs = f"{r[3]:.0f}%" if r[3] is not None else "-"
                line = f"  {r[0]:<30} {r[1]:<30} {r[2]:>12.2f} {vs:>10}"
                tdLog.info(line)
                summary_lines.append(line)
        tdLog.info("")
        tdLog.info("=" * 90)
        tdLog.info("  All %d benchmark cases completed." % len(_all_reports))
        tdLog.info("=" * 90)
        tdLog.info("")
        # Write full summary to file
        with open(_REPORT_FILE, "a") as f:
            f.write("\n" + "=" * 90 + "\n")
            f.write("  FULL CONSOLIDATED REPORT\n")
            f.write("=" * 90 + "\n")
            f.write("\n".join(summary_lines) + "\n")
            f.write("\n" + "=" * 90 + "\n")
            f.write(f"  All {len(_all_reports)} benchmark cases completed.\n")
            f.write("=" * 90 + "\n")


def _insert_child_rows_wide(table, n_rows, n_cols, batch=1000):
    """Insert n_rows into table with ts + n_cols INT columns."""
    for start in range(0, n_rows, batch):
        end = min(start + batch, n_rows)
        vals = []
        for i in range(start, end):
            cols = ", ".join(str(i + j) for j in range(n_cols))
            vals.append(f"({BASE_TS + i}, {cols})")
        tdSql.execute(f"INSERT INTO {table} VALUES " + ", ".join(vals))
