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
"""Performance benchmark: tag-ref R2 tag query types.

Compares literal-tag vs tag-ref across 7 tag query shapes:
  tag_eq, tag_in_10, tag_in_50, tag_range, tag_group, tag_distinct, tag_order

Environment: children=100, rows/child=1000, data_cols=5, tag_cols=3, vgroups=2
Report written to /tmp/perf_tag_ref_r2_tag_queries_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport

REPORT_FILE = "/tmp/perf_tag_ref_r2_tag_queries_report.txt"
rpt = PerfReport(REPORT_FILE)

TAG_QUERIES = [
    ("tag_eq",       "SELECT * FROM {stb} WHERE t0 = 50"),
    ("tag_in_10",    "SELECT * FROM {stb} WHERE t0 IN (0,10,20,30,40,50,60,70,80,90)"),
    ("tag_in_50",    "SELECT * FROM {stb} WHERE t0 IN (" + ",".join(str(i) for i in range(0, 500, 10)) + ")"),
    ("tag_range",    "SELECT * FROM {stb} WHERE t0 >= 200 AND t0 < 500"),
    ("tag_group",    "SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
    ("tag_distinct", "SELECT DISTINCT t0 FROM {stb}"),
    ("tag_order",    "SELECT * FROM {stb} ORDER BY t0 LIMIT 100"),
]


class TestPerfTagRefR2TagQueries:

    def setup_class(cls):
        apply_perf_flags()
        rpt.header("tag-ref R2: tag query types (literal vs tag-ref)")

        from tag_ref_perf_util import build_tag_ref_env
        env = build_tag_ref_env(
            db="pf_r2_tagq",
            children=100,
            rows_per_child=1000,
            data_cols=5,
            tag_cols=3,
            vgroups=2,
        )
        cls._lit = f"{env['db']}.{env['vstb_literal']}"
        cls._tref = f"{env['db']}.{env['vstb_tagref']}"
        cls._db = env["db"]

        # Pre-run all queries
        cls._results = {}
        for qname, qtpl in TAG_QUERIES:
            lit_ms = median(bench(qtpl.format(stb=cls._lit)))
            tref_ms = median(bench(qtpl.format(stb=cls._tref)))
            cls._results[qname] = (lit_ms, tref_ms)

    def test_01_tag_query_matrix(self):
        """Tag-ref R2: tag query shape comparison matrix

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        tdSql.execute(f"USE {self._db};")
        rpt.emit("\n=== Tag Query Types: Literal vs Tag-ref ===")
        rpt.emit(f"  Environment: children=100, rows/child=1000, data_cols=5, tag_cols=3, vgroups=2")
        rpt.emit("")
        rpt.emit(f"  {'Shape':<16} {'Literal(ms)':>12} {'TagRef(ms)':>12} {'Delta(ms)':>12} {'Overhead':>10}")
        rpt.emit("  " + "-" * 68)

        for qname, _ in TAG_QUERIES:
            lit_ms, tref_ms = self._results[qname]
            delta = tref_ms - lit_ms
            over = f"{delta / lit_ms * 100:+.1f}%" if lit_ms > 0 else "-"
            rpt.emit(f"  {qname:<16} {lit_ms:>12.2f} {tref_ms:>12.2f} {delta:>12.2f} {over:>10}")

        rpt.flush()

    def test_02_overhead_ranking(self):
        """Tag-ref R2: rank query shapes by overhead percentage

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        tdSql.execute(f"USE {self._db};")
        ranked = []
        for qname, _ in TAG_QUERIES:
            lit_ms, tref_ms = self._results[qname]
            pct = (tref_ms - lit_ms) / lit_ms * 100 if lit_ms > 0 else 0
            ranked.append((qname, lit_ms, tref_ms, pct))
        ranked.sort(key=lambda x: x[3], reverse=True)

        rpt.emit("\n=== Overhead Ranking (highest first) ===")
        rpt.emit(f"  {'Rank':<6} {'Shape':<16} {'Literal(ms)':>12} {'TagRef(ms)':>12} {'Overhead':>10}")
        rpt.emit("  " + "-" * 60)
        for i, (qname, lit_ms, tref_ms, pct) in enumerate(ranked, 1):
            rpt.emit(f"  {i:<6} {qname:<16} {lit_ms:>12.2f} {tref_ms:>12.2f} {pct:>+9.1f}%")

        rpt.flush()

    def test_03_selectivity_analysis(self):
        """Tag-ref R2: tag filter selectivity vs overhead

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        tdSql.execute(f"USE {self._db};")
        # tag_eq = 1/100 selectivity, tag_in_10 = 10/100, tag_in_50 = 50/100, tag_range = 30/100
        selectivity_map = {
            "tag_eq": "1%",
            "tag_in_10": "10%",
            "tag_in_50": "50%",
            "tag_range": "30%",
        }

        rpt.emit("\n=== Selectivity vs Overhead ===")
        rpt.emit(f"  {'Shape':<16} {'Selectivity':>12} {'Literal(ms)':>12} {'TagRef(ms)':>12} {'Overhead':>10}")
        rpt.emit("  " + "-" * 68)
        for qname, sel in selectivity_map.items():
            lit_ms, tref_ms = self._results[qname]
            over = f"{(tref_ms - lit_ms) / lit_ms * 100:+.1f}%" if lit_ms > 0 else "-"
            rpt.emit(f"  {qname:<16} {sel:>12} {lit_ms:>12.2f} {tref_ms:>12.2f} {over:>10}")

        rpt.flush()

    def test_zz_summary(self):
        """Summary

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n" + "=" * 100)
        rpt.emit("tag-ref R2 tag queries: benchmark complete")
        rpt.emit("=" * 100)
        rpt.footer()
