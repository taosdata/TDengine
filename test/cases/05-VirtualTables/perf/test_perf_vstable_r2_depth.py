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
"""Performance benchmark: vstable-ref-vstable depth scaling (C-R2).

Measures L0 through L8 to quantify per-layer marginal cost.
Fixed parameters: children=20, rows=1000, data_cols=5, tag_cols=3, vgroups=2.

Report written to /tmp/perf_vstable_r2_depth_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from vstable_perf_util import build_vstable_chain

REPORT_FILE = "/tmp/perf_vstable_r2_depth_report.txt"
rpt = PerfReport(REPORT_FILE)

DEPTHS = [0, 1, 2, 4, 8]
QUERIES = [
    ("Q1 SELECT *", "SELECT * FROM {stb}"),
    ("Q2 COUNT", "SELECT COUNT(*) FROM {stb}"),
    ("Q3 SUM+AVG", "SELECT SUM(c0), AVG(c1) FROM {stb}"),
    ("Q4 WHERE tag", "SELECT * FROM {stb} WHERE t0 = 0"),
    ("Q5 GROUP BY", "SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
]


class TestPerfVstableR2Depth:

    def setup_class(cls):
        apply_perf_flags()
        rpt.header("vstable R2: depth scaling (L0-L8)")

        cls._db = "pf_vstable_r2_depth"
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

    def test_01_depth_matrix(self):
        """Vstable R2 depth: depth x query matrix with per-layer deltas

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        tdSql.execute(f"USE {self._db};")
        rpt.emit("\n=== Depth x Query Matrix ===")
        rpt.emit(f"  {'Depth':<8} {'Query':<18} {'Median(ms)':>12} {'vs L0':>10} {'Delta Prev':>12}")
        rpt.emit("  " + "-" * 66)

        for qname, _ in QUERIES:
            base_ms = None
            prev_ms = None
            for depth in DEPTHS:
                ms = self._results[qname][depth]
                if base_ms is None:
                    base_ms = ms
                vs_l0 = f"{ms / base_ms * 100:.0f}%" if base_ms > 0 else "-"
                delta = f"{ms - prev_ms:+.2f}" if prev_ms is not None else "-"
                rpt.emit(f"  L{depth:<7} {qname:<18} {ms:>12.2f} {vs_l0:>10} {delta:>12}")
                prev_ms = ms
        rpt.flush()

    def test_02_marginal_cost_summary(self):
        """Vstable R2 depth: marginal cost per layer

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        tdSql.execute(f"USE {self._db};")
        rpt.emit("\n=== Marginal Cost Summary ===")
        rpt.emit(f"  {'Query':<18} {'L1-L0(ms)':>12} {'L2-L1(ms)':>12} {'Avg L2+(ms)':>12} {'Max L2+(ms)':>12}")
        rpt.emit("  " + "-" * 70)

        for qname, _ in QUERIES:
            values = [self._results[qname][d] for d in DEPTHS]
            deltas = [values[i] - values[i - 1] for i in range(1, len(values))]
            # deltas[0] = L1-L0, deltas[1:] = pure vstable ref deltas
            pure_deltas = deltas[1:]
            avg_l2_plus = sum(pure_deltas) / len(pure_deltas)
            max_l2_plus = max(pure_deltas)
            rpt.emit(f"  {qname:<18} {deltas[0]:>12.2f} {deltas[1]:>12.2f} {avg_l2_plus:>12.2f} {max_l2_plus:>12.2f}")
        rpt.flush()

    def test_zz_summary(self):
        """Write final report summary

        Perf benchmark measurement.

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
