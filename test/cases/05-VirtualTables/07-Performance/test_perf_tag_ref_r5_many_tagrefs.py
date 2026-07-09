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
"""Performance benchmark: tag-ref R5 many tag-ref columns.

Measures how overhead grows with the number of tag-ref columns.
Fixed children=50, rows=1000, sweep tag_cols=[1, 3, 5, 10, 20, 50].

Queries: Q2 (COUNT), Q5 (WHERE tag), Q6 (GROUP BY tag)
Each tag_cols value gets its own database.

Report written to /tmp/perf_tag_ref_r5_many_tagrefs_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from tag_ref_perf_util import build_tag_ref_env

REPORT_FILE = "/tmp/perf_tag_ref_r5_many_tagrefs_report.txt"
rpt = PerfReport(REPORT_FILE)

CHILDREN = 50
ROWS_PER_CHILD = 1000
DATA_COLS = 5
TAG_COLS_SET = [1, 3, 5, 10, 20, 50]
VGROUPS = 2

QUERIES = [
    ("Q2 COUNT",    "SELECT COUNT(*) FROM {stb}"),
    ("Q5 WHERE t0", "SELECT * FROM {stb} WHERE t0 = 0"),
    ("Q6 GROUP t0", "SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
]


class TestPerfTagRefR5ManyTagrefs:

    def setup_class(cls):
        apply_perf_flags()
        rpt.header("tag-ref R5: many tag-ref columns (children=50, rows=1000)")
        rpt.emit(f"  Tag columns sweep: {TAG_COLS_SET}")
        rpt.emit(f"  children={CHILDREN}, rows/child={ROWS_PER_CHILD}, data_cols={DATA_COLS}, vgroups={VGROUPS}")
        rpt.emit(f"  Queries: Q2(COUNT), Q5(WHERE t0), Q6(GROUP BY t0)")

        cls._sweep = []
        for n_tags in TAG_COLS_SET:
            db_name = f"pf_r5_t{n_tags}"
            env = build_tag_ref_env(
                db=db_name,
                children=CHILDREN,
                rows_per_child=ROWS_PER_CHILD,
                data_cols=DATA_COLS,
                tag_cols=n_tags,
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
                "tag_cols": n_tags,
                "db": db_name,
                "results": results,
            })

    def test_01_tagref_count_matrix(self):
        """Tag-ref R5: overhead growth with tag-ref count

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Tag-ref Count x Query Matrix ===")
        rpt.emit(f"  {'TagCols':>10} | {'Q2 Lit':>10} {'Q2 TRef':>10} {'Q2 Over%':>10} | "
                 f"{'Q5 Lit':>10} {'Q5 TRef':>10} {'Q5 Over%':>10} | "
                 f"{'Q6 Lit':>10} {'Q6 TRef':>10} {'Q6 Over%':>10}")
        rpt.emit("  " + "-" * 120)

        for entry in self._sweep:
            n = entry["tag_cols"]
            res = entry["results"]
            parts = []
            for qname in ["Q2 COUNT", "Q5 WHERE t0", "Q6 GROUP t0"]:
                lit_ms, tref_ms = res[qname]
                over = f"{(tref_ms - lit_ms) / lit_ms * 100:+.1f}%" if lit_ms > 0 else "-"
                parts.append(f"{lit_ms:>10.2f} {tref_ms:>10.2f} {over:>10}")
            rpt.emit(f"  {n:>10} | " + " | ".join(parts))

        rpt.flush()

    def test_02_q5_overhead_trend(self):
        """Tag-ref R5: Q5 (WHERE tag) overhead growth per tag-ref column

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Q5 (WHERE tag) Overhead vs Tag-ref Count ===")
        rpt.emit(f"  {'TagCols':>10} {'Literal(ms)':>14} {'TagRef(ms)':>14} {'Delta(ms)':>14} "
                 f"{'Over%':>10} {'Per-Tag(ms)':>14}")
        rpt.emit("  " + "-" * 82)

        for entry in self._sweep:
            n = entry["tag_cols"]
            lit_ms, tref_ms = entry["results"]["Q5 WHERE t0"]
            delta = tref_ms - lit_ms
            over = f"{delta / lit_ms * 100:+.1f}%" if lit_ms > 0 else "-"
            per_tag = delta / n if n > 0 else 0
            rpt.emit(f"  {n:>10} {lit_ms:>14.2f} {tref_ms:>14.2f} {delta:>14.2f} "
                     f"{over:>10} {per_tag:>14.4f}")

        rpt.flush()

    def test_03_q6_overhead_trend(self):
        """Tag-ref R5: Q6 (GROUP BY tag) overhead growth per tag-ref column

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Q6 (GROUP BY tag) Overhead vs Tag-ref Count ===")
        rpt.emit(f"  {'TagCols':>10} {'Literal(ms)':>14} {'TagRef(ms)':>14} {'Delta(ms)':>14} "
                 f"{'Over%':>10} {'Per-Tag(ms)':>14}")
        rpt.emit("  " + "-" * 82)

        for entry in self._sweep:
            n = entry["tag_cols"]
            lit_ms, tref_ms = entry["results"]["Q6 GROUP t0"]
            delta = tref_ms - lit_ms
            over = f"{delta / lit_ms * 100:+.1f}%" if lit_ms > 0 else "-"
            per_tag = delta / n if n > 0 else 0
            rpt.emit(f"  {n:>10} {lit_ms:>14.2f} {tref_ms:>14.2f} {delta:>14.2f} "
                     f"{over:>10} {per_tag:>14.4f}")

        rpt.flush()

    def test_04_linearity_check(self):
        """Tag-ref R5: linearity check -- is overhead proportional to tag-ref count?

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Linearity Check: Q5 delta / tag-ref count ===")
        rpt.emit(f"  {'TagCols':>10} {'Q5 Delta(ms)':>14} {'Per-Tag(ms)':>14} {'Ratio vs t=1':>14}")
        rpt.emit("  " + "-" * 58)

        base_per_tag = None
        for entry in self._sweep:
            n = entry["tag_cols"]
            lit_ms, tref_ms = entry["results"]["Q5 WHERE t0"]
            delta = tref_ms - lit_ms
            per_tag = delta / n if n > 0 else 0
            if base_per_tag is None:
                base_per_tag = per_tag
            ratio = f"{per_tag / base_per_tag:.2f}x" if base_per_tag and base_per_tag > 0 else "-"
            rpt.emit(f"  {n:>10} {delta:>14.2f} {per_tag:>14.4f} {ratio:>14}")

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
        rpt.emit("tag-ref R5 many tagrefs: benchmark complete")
        rpt.emit("=" * 100)
        rpt.footer()
