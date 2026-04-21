###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""Performance benchmark: tag-ref R1 baseline.

Compares three baselines across Q1-Q8:
  1. source stable (physical)
  2. literal-tag vstable
  3. tag-ref vstable

Environment: children=20, rows/child=1000, data_cols=5, tag_cols=3, vgroups=2
Report written to /tmp/perf_tag_ref_r1_baseline_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport

REPORT_FILE = "/tmp/perf_tag_ref_r1_baseline_report.txt"
rpt = PerfReport(REPORT_FILE)

QUERIES = [
    ("Q1 SELECT *", "SELECT * FROM {stb}"),
    ("Q2 COUNT",    "SELECT COUNT(*) FROM {stb}"),
    ("Q3 SUM+AVG", "SELECT SUM(c0), AVG(c1) FROM {stb}"),
    ("Q4 WHERE dt", "SELECT * FROM {stb} WHERE c0 >= 500"),
    ("Q5 WHERE t0", "SELECT * FROM {stb} WHERE t0 = 0"),
    ("Q6 GROUP t0", "SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
    ("Q7 DIST t0",  "SELECT DISTINCT t0 FROM {stb}"),
    ("Q8 LAST c0",  "SELECT LAST(c0) FROM {stb}"),
]


class TestPerfTagRefR1Baseline:

    def setup_class(cls):
        apply_perf_flags()
        rpt.header("tag-ref R1: baseline (src vs literal vs tag-ref)")

        from tag_ref_perf_util import build_tag_ref_env
        env = build_tag_ref_env(
            db="pf_r1_base",
            children=20,
            rows_per_child=1000,
            data_cols=5,
            tag_cols=3,
            vgroups=2,
        )
        cls._src = f"{env['db']}.{env['src_stb']}"
        cls._lit = f"{env['db']}.{env['vstb_literal']}"
        cls._tref = f"{env['db']}.{env['vstb_tagref']}"
        cls._db = env["db"]

        # Pre-run all queries and store median results
        cls._results = {}
        for qname, qtpl in QUERIES:
            src_ms = median(bench(qtpl.format(stb=cls._src)))
            lit_ms = median(bench(qtpl.format(stb=cls._lit)))
            tref_ms = median(bench(qtpl.format(stb=cls._tref)))
            cls._results[qname] = (src_ms, lit_ms, tref_ms)

    def test_01_query_matrix(self):
        """Tag-ref R1 baseline: 3-column query matrix with overhead percentages

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        tdSql.execute(f"USE {self._db};")
        rpt.emit("\n=== Query Matrix: Source vs Literal vs Tag-ref ===")
        rpt.emit(f"  Environment: children=20, rows/child=1000, data_cols=5, tag_cols=3, vgroups=2")
        rpt.emit("")
        rpt.emit(f"  {'Query':<16} {'Source(ms)':>12} {'Literal(ms)':>12} {'TagRef(ms)':>12} "
                 f"{'Lit/Src':>10} {'TRef/Src':>10} {'TRef/Lit':>10}")
        rpt.emit("  " + "-" * 92)

        for qname, _ in QUERIES:
            src_ms, lit_ms, tref_ms = self._results[qname]
            lit_src = f"{lit_ms / src_ms * 100:.1f}%" if src_ms > 0 else "-"
            tref_src = f"{tref_ms / src_ms * 100:.1f}%" if src_ms > 0 else "-"
            tref_lit = f"{tref_ms / lit_ms * 100:.1f}%" if lit_ms > 0 else "-"
            rpt.emit(f"  {qname:<16} {src_ms:>12.2f} {lit_ms:>12.2f} {tref_ms:>12.2f} "
                     f"{lit_src:>10} {tref_src:>10} {tref_lit:>10}")

        rpt.flush()

    def test_02_fixed_cost(self):
        """Tag-ref R1 baseline: absolute fixed cost per query variant

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        tdSql.execute(f"USE {self._db};")
        rpt.emit("\n=== Fixed Cost (absolute ms) ===")
        rpt.emit(f"  {'Query':<16} {'lit-src(ms)':>14} {'tref-lit(ms)':>14} {'tref-src(ms)':>14}")
        rpt.emit("  " + "-" * 64)

        for qname, _ in QUERIES:
            src_ms, lit_ms, tref_ms = self._results[qname]
            rpt.emit(f"  {qname:<16} {lit_ms - src_ms:>14.2f} "
                     f"{tref_ms - lit_ms:>14.2f} {tref_ms - src_ms:>14.2f}")

        rpt.flush()

    def test_03_data_vs_tag_queries(self):
        """Tag-ref R1 baseline: separate data-query vs tag-query overhead analysis

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        tdSql.execute(f"USE {self._db};")
        data_queries = ["Q1 SELECT *", "Q2 COUNT", "Q3 SUM+AVG", "Q4 WHERE dt", "Q8 LAST c0"]
        tag_queries = ["Q5 WHERE t0", "Q6 GROUP t0", "Q7 DIST t0"]

        rpt.emit("\n=== Data Queries: Tag-ref Overhead ===")
        rpt.emit(f"  {'Query':<16} {'Literal(ms)':>12} {'TagRef(ms)':>12} {'Overhead':>10}")
        rpt.emit("  " + "-" * 56)
        for qname in data_queries:
            _, lit_ms, tref_ms = self._results[qname]
            over = f"{(tref_ms - lit_ms) / lit_ms * 100:+.1f}%" if lit_ms > 0 else "-"
            rpt.emit(f"  {qname:<16} {lit_ms:>12.2f} {tref_ms:>12.2f} {over:>10}")

        rpt.emit("\n=== Tag Queries: Tag-ref Overhead ===")
        rpt.emit(f"  {'Query':<16} {'Literal(ms)':>12} {'TagRef(ms)':>12} {'Overhead':>10}")
        rpt.emit("  " + "-" * 56)
        for qname in tag_queries:
            _, lit_ms, tref_ms = self._results[qname]
            over = f"{(tref_ms - lit_ms) / lit_ms * 100:+.1f}%" if lit_ms > 0 else "-"
            rpt.emit(f"  {qname:<16} {lit_ms:>12.2f} {tref_ms:>12.2f} {over:>10}")

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
        rpt.emit("tag-ref R1 baseline: benchmark complete")
        rpt.emit("=" * 100)
        rpt.footer()
