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
"""Performance benchmark: vstable-ref-vstable baseline (C-R1).

Compares physical source stable (L0) vs first vstable layer (L1) vs
second vstable layer (L2).  Fixed parameters: children=20, rows=1000,
data_cols=5, tag_cols=3, vgroups=2.

Report written to /tmp/perf_vstable_r1_baseline_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from vstable_perf_util import build_vstable_chain

REPORT_FILE = "/tmp/perf_vstable_r1_baseline_report.txt"
rpt = PerfReport(REPORT_FILE)

DEPTHS = [0, 1, 2]
QUERIES = [
    ("Q1 SELECT *", "SELECT * FROM {stb}"),
    ("Q2 COUNT", "SELECT COUNT(*) FROM {stb}"),
    ("Q3 SUM+AVG", "SELECT SUM(c0), AVG(c1) FROM {stb}"),
    ("Q4 WHERE tag", "SELECT * FROM {stb} WHERE t0 = 0"),
    ("Q5 GROUP BY t0", "SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
    ("Q6 DISTINCT t0", "SELECT DISTINCT t0 FROM {stb}"),
    ("Q7 ORDER BY t0", "SELECT * FROM {stb} ORDER BY t0 LIMIT 100"),
    ("Q8 LAST(c0)", "SELECT LAST(c0) FROM {stb}"),
]


class TestPerfVstableR1Baseline:

    def setup_class(cls):
        apply_perf_flags()
        rpt.header("vstable R1: baseline (L0 vs L1 vs L2)")

        cls._db = "pf_vstable_r1_base"
        cls._chain = build_vstable_chain(
            db=cls._db,
            depths=DEPTHS,
            children=20,
            rows_per_child=1000,
            data_cols=5,
            tag_cols=3,
            vgroups=2,
            use_tag_ref=False,
        )

        # Pre-run all queries and cache medians
        cls._results = {}
        for qname, qtpl in QUERIES:
            cls._results[qname] = {}
            for depth in DEPTHS:
                stb = cls._chain[depth]
                cls._results[qname][depth] = median(bench(qtpl.format(stb=stb)))

    def test_01_query_matrix(self):
        """Vstable R1 baseline: L0 vs L1 vs L2 query matrix

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        tdSql.execute(f"USE {self._db};")
        rpt.emit("\n=== Query Matrix (L0 vs L1 vs L2) ===")
        rpt.emit(f"  {'Depth':<14} {'Query':<18} {'Median(ms)':>12} {'vs L0':>10} {'Delta Prev':>12}")
        rpt.emit("  " + "-" * 74)

        depth_names = {0: "L0 source", 1: "L1 vstb-1", 2: "L2 vstb-2"}
        for qname, _ in QUERIES:
            base_ms = None
            prev_ms = None
            for depth in DEPTHS:
                ms = self._results[qname][depth]
                if base_ms is None:
                    base_ms = ms
                vs_l0 = f"{ms / base_ms * 100:.0f}%" if base_ms > 0 else "-"
                delta = f"{ms - prev_ms:+.2f}" if prev_ms is not None else "-"
                rpt.emit(f"  {depth_names[depth]:<14} {qname:<18} {ms:>12.2f} {vs_l0:>10} {delta:>12}")
                prev_ms = ms
        rpt.flush()

    def test_02_entry_cost_summary(self):
        """Vstable R1 baseline: entry-cost summary

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        tdSql.execute(f"USE {self._db};")
        rpt.emit("\n=== Entry Cost Summary ===")
        rpt.emit(f"  {'Query':<18} {'L1-L0(ms)':>12} {'L2-L1(ms)':>12} {'L2-L0(ms)':>12}")
        rpt.emit("  " + "-" * 60)

        for qname, _ in QUERIES:
            l0 = self._results[qname][0]
            l1 = self._results[qname][1]
            l2 = self._results[qname][2]
            rpt.emit(f"  {qname:<18} {l1 - l0:>12.2f} {l2 - l1:>12.2f} {l2 - l0:>12.2f}")
        rpt.flush()

    def test_zz_summary(self):
        """Summary

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.footer()
        tdLog.info(f"Report: {REPORT_FILE}")
