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
"""Performance benchmark: tag-ref R6 cross-db tag-ref.

Compares same-db vs cross-db tag-ref performance to measure
the RPC overhead of cross-database tag references.

Environment: children=50, rows/child=1000, data_cols=5, tag_cols=3, vgroups=2
Queries: Q2 (COUNT), Q5 (WHERE tag), Q6 (GROUP BY tag)

Report written to /tmp/perf_tag_ref_r6_cross_db_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from tag_ref_perf_util import build_tag_ref_env

REPORT_FILE = "/tmp/perf_tag_ref_r6_cross_db_report.txt"
rpt = PerfReport(REPORT_FILE)

CHILDREN = 50
ROWS_PER_CHILD = 1000
DATA_COLS = 5
TAG_COLS = 3
VGROUPS = 2

QUERIES = [
    ("Q2 COUNT",    "SELECT COUNT(*) FROM {stb}"),
    ("Q5 WHERE t0", "SELECT * FROM {stb} WHERE t0 = 0"),
    ("Q6 GROUP t0", "SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
]


class TestPerfTagRefR6CrossDb:

    def setup_class(cls):
        apply_perf_flags()
        rpt.header("tag-ref R6: cross-db tag-ref (same-db vs cross-db)")
        rpt.emit(f"  Environment: children={CHILDREN}, rows/child={ROWS_PER_CHILD}, "
                 f"data_cols={DATA_COLS}, tag_cols={TAG_COLS}, vgroups={VGROUPS}")
        rpt.emit(f"  Queries: Q2(COUNT), Q5(WHERE tag), Q6(GROUP BY tag)")

        # Same-db env
        env_same = build_tag_ref_env(
            db="pf_r6_same",
            children=CHILDREN,
            rows_per_child=ROWS_PER_CHILD,
            data_cols=DATA_COLS,
            tag_cols=TAG_COLS,
            vgroups=VGROUPS,
            tag_db=None,
        )
        cls._same_lit = f"{env_same['db']}.{env_same['vstb_literal']}"
        cls._same_tref = f"{env_same['db']}.{env_same['vstb_tagref']}"
        cls._same_db = env_same["db"]

        # Cross-db env
        env_cross = build_tag_ref_env(
            db="pf_r6_cross",
            children=CHILDREN,
            rows_per_child=ROWS_PER_CHILD,
            data_cols=DATA_COLS,
            tag_cols=TAG_COLS,
            vgroups=VGROUPS,
            tag_db="pf_r6_xdb",
        )
        cls._cross_lit = f"{env_cross['db']}.{env_cross['vstb_literal']}"
        cls._cross_tref = f"{env_cross['db']}.{env_cross['vstb_tagref']}"
        cls._cross_db = env_cross["db"]

        # Pre-run all queries
        cls._results = {}
        for qname, qtpl in QUERIES:
            same_lit_ms = median(bench(qtpl.format(stb=cls._same_lit)))
            same_tref_ms = median(bench(qtpl.format(stb=cls._same_tref)))
            cross_lit_ms = median(bench(qtpl.format(stb=cls._cross_lit)))
            cross_tref_ms = median(bench(qtpl.format(stb=cls._cross_tref)))
            cls._results[qname] = {
                "same_lit": same_lit_ms,
                "same_tref": same_tref_ms,
                "cross_lit": cross_lit_ms,
                "cross_tref": cross_tref_ms,
            }

    def test_01_same_vs_cross_matrix(self):
        """Tag-ref R6: same-db vs cross-db comparison matrix

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Same-DB vs Cross-DB Tag-ref ===")
        rpt.emit(f"  {'Query':<16} | {'Same Lit':>10} {'Same TRef':>10} {'Over%':>10} | "
                 f"{'Cross Lit':>10} {'Cross TRef':>10} {'Over%':>10} | {'Cross Penalty':>14}")
        rpt.emit("  " + "-" * 110)

        for qname, _ in QUERIES:
            r = self._results[qname]
            same_over = f"{(r['same_tref'] - r['same_lit']) / r['same_lit'] * 100:+.1f}%" if r['same_lit'] > 0 else "-"
            cross_over = f"{(r['cross_tref'] - r['cross_lit']) / r['cross_lit'] * 100:+.1f}%" if r['cross_lit'] > 0 else "-"
            penalty = r['cross_tref'] - r['same_tref']
            rpt.emit(f"  {qname:<16} | {r['same_lit']:>10.2f} {r['same_tref']:>10.2f} {same_over:>10} | "
                     f"{r['cross_lit']:>10.2f} {r['cross_tref']:>10.2f} {cross_over:>10} | {penalty:>+14.2f} ms")

        rpt.flush()

    def test_02_cross_db_overhead_detail(self):
        """Tag-ref R6: cross-db RPC overhead breakdown for tag queries

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Cross-DB RPC Overhead Detail ===")
        rpt.emit(f"  {'Query':<16} {'Same TRef(ms)':>14} {'Cross TRef(ms)':>16} "
                 f"{'Cross Penalty':>14} {'Penalty %':>12}")
        rpt.emit("  " + "-" * 78)

        for qname, _ in QUERIES:
            r = self._results[qname]
            penalty = r['cross_tref'] - r['same_tref']
            pct = f"{penalty / r['same_tref'] * 100:+.1f}%" if r['same_tref'] > 0 else "-"
            rpt.emit(f"  {qname:<16} {r['same_tref']:>14.2f} {r['cross_tref']:>16.2f} "
                     f"{penalty:>+14.2f} {pct:>12}")

        rpt.flush()

    def test_03_literal_baseline_check(self):
        """Tag-ref R6: verify literal-tag performance is similar in both envs

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Literal-tag Baseline Check (should be similar) ===")
        rpt.emit(f"  {'Query':<16} {'Same Lit(ms)':>14} {'Cross Lit(ms)':>16} {'Delta':>12} {'Delta%':>10}")
        rpt.emit("  " + "-" * 74)

        for qname, _ in QUERIES:
            r = self._results[qname]
            delta = r['cross_lit'] - r['same_lit']
            pct = f"{delta / r['same_lit'] * 100:+.1f}%" if r['same_lit'] > 0 else "-"
            rpt.emit(f"  {qname:<16} {r['same_lit']:>14.2f} {r['cross_lit']:>16.2f} {delta:>+12.2f} {pct:>10}")

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
        rpt.emit("tag-ref R6 cross-db: benchmark complete")
        rpt.emit("=" * 100)
        rpt.footer()
