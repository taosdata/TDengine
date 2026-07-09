###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies,
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""Performance benchmark: tag-ref R3 many tables, little data.

Measures how tag-ref overhead scales with child table count when
each child has very little data (100 rows).

Children sweep: [50, 100, 200, 500, 1000]
Queries: Q2 (COUNT), Q5 (WHERE tag), Q6 (GROUP BY tag)
Each children count gets its own database.

Report written to /tmp/perf_tag_ref_r3_many_tables_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from tag_ref_perf_util import build_tag_ref_env

REPORT_FILE = "/tmp/perf_tag_ref_r3_many_tables_report.txt"
rpt = PerfReport(REPORT_FILE)

CHILDREN_SET = [50, 100, 200, 500, 1000]
ROWS_PER_CHILD = 100
DATA_COLS = 5
TAG_COLS = 3
VGROUPS = 4

QUERIES = [
    ("Q2 COUNT",  "SELECT COUNT(*) FROM {stb}"),
    ("Q5 WHERE t0", "SELECT * FROM {stb} WHERE t0 = 0"),
    ("Q6 GROUP t0", "SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
]


class TestPerfTagRefR3ManyTables:

    def setup_class(cls):
        apply_perf_flags()
        rpt.header("tag-ref R3: many tables, little data (rows=100)")
        rpt.emit(f"  Children sweep: {CHILDREN_SET}")
        rpt.emit(f"  rows/child={ROWS_PER_CHILD}, data_cols={DATA_COLS}, tag_cols={TAG_COLS}, vgroups={VGROUPS}")
        rpt.emit(f"  Queries: Q2(COUNT), Q5(WHERE tag), Q6(GROUP BY tag)")

        # Build env and collect results for each children count
        cls._sweep = []
        for n_children in CHILDREN_SET:
            db_name = f"pf_r3_c{n_children}"
            env = build_tag_ref_env(
                db=db_name,
                children=n_children,
                rows_per_child=ROWS_PER_CHILD,
                data_cols=DATA_COLS,
                tag_cols=TAG_COLS,
                vgroups=VGROUPS,
            )
            lit = f"{env['db']}.{env['vstb_literal']}"
            tref = f"{env['db']}.{env['vstb_tagref']}"

            results = {}
            for qname, qtpl in QUERIES:
                lit_ms = median(bench(qtpl.format(stb=lit)))
                tref_ms = median(bench(qtpl.format(stb=tref)))
                results[qname] = (lit_ms, tref_ms)

            cls._sweep.append({
                "children": n_children,
                "db": db_name,
                "results": results,
            })

    def test_01_scaling_matrix(self):
        """Tag-ref R3: overhead scaling with children count

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Overhead Scaling with Children Count ===")
        rpt.emit(f"  {'Children':>10} | {'Q2 Literal':>12} {'Q2 TagRef':>12} {'Q2 Over%':>10} | "
                 f"{'Q5 Literal':>12} {'Q5 TagRef':>12} {'Q5 Over%':>10} | "
                 f"{'Q6 Literal':>12} {'Q6 TagRef':>12} {'Q6 Over%':>10}")
        rpt.emit("  " + "-" * 130)

        for entry in self._sweep:
            n = entry["children"]
            res = entry["results"]
            parts = []
            for qname in ["Q2 COUNT", "Q5 WHERE t0", "Q6 GROUP t0"]:
                lit_ms, tref_ms = res[qname]
                over = f"{(tref_ms - lit_ms) / lit_ms * 100:+.1f}%" if lit_ms > 0 else "-"
                parts.append(f"{lit_ms:>12.2f} {tref_ms:>12.2f} {over:>10}")
            rpt.emit(f"  {n:>10} | " + " | ".join(parts))

        rpt.flush()

    def test_02_overhead_trend(self):
        """Tag-ref R3: absolute overhead trend (delta ms) with children count

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Absolute Overhead Trend (tag-ref - literal, ms) ===")
        rpt.emit(f"  {'Children':>10} | {'Q2 Delta(ms)':>14} | {'Q5 Delta(ms)':>14} | {'Q6 Delta(ms)':>14}")
        rpt.emit("  " + "-" * 66)

        for entry in self._sweep:
            n = entry["children"]
            res = entry["results"]
            deltas = []
            for qname in ["Q2 COUNT", "Q5 WHERE t0", "Q6 GROUP t0"]:
                lit_ms, tref_ms = res[qname]
                deltas.append(tref_ms - lit_ms)
            rpt.emit(f"  {n:>10} | {deltas[0]:>14.2f} | {deltas[1]:>14.2f} | {deltas[2]:>14.2f}")

        rpt.flush()

    def test_03_linear_check(self):
        """Tag-ref R3: check if Q5 overhead is approximately linear with children count

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Linearity Check: Q5 (WHERE tag) overhead per child ===")
        rpt.emit(f"  {'Children':>10} {'Q5 Delta(ms)':>14} {'Per-Child(ms)':>14} {'Ratio vs 50':>14}")
        rpt.emit("  " + "-" * 58)

        base_delta = None
        for entry in self._sweep:
            n = entry["children"]
            lit_ms, tref_ms = entry["results"]["Q5 WHERE t0"]
            delta = tref_ms - lit_ms
            per_child = delta / n if n > 0 else 0
            if base_delta is None:
                base_delta = delta
            ratio = f"{delta / base_delta:.2f}x" if base_delta and base_delta > 0 else "-"
            rpt.emit(f"  {n:>10} {delta:>14.2f} {per_child:>14.4f} {ratio:>14}")

        rpt.flush()

    def test_zz_summary(self):
        """Summary

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n" + "=" * 100)
        rpt.emit("tag-ref R3 many tables: benchmark complete")
        rpt.emit("=" * 100)
        rpt.footer()
