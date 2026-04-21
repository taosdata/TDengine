###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies,
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""Performance benchmark: vchild-ref-vchild depth scaling (B-R2).

Measures L0 through L32 to quantify per-layer marginal cost of the
virtual-child reference chain.

Report written to /tmp/perf_vchild_r2_depth_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from vchild_perf_util import build_vchild_chain, half_filter_value

DEPTHS = [0, 1, 2, 4, 8, 16, 32]
ROWS_PER_CHILD = 10000
DATA_COLS = 5
REPORT_FILE = "/tmp/perf_vchild_r2_depth_report.txt"

rpt = PerfReport(REPORT_FILE)

FILTER_VAL = half_filter_value(ROWS_PER_CHILD)
QUERIES = [
    ("SELECT *", "SELECT * FROM {tbl}"),
    ("COUNT(*)", "SELECT COUNT(*) FROM {tbl}"),
    ("SUM+AVG", "SELECT SUM(c0), AVG(c1) FROM {tbl}"),
    ("WHERE data", f"SELECT * FROM {{tbl}} WHERE c0 >= {FILTER_VAL}"),
    ("LAST", "SELECT LAST(c0) FROM {tbl}"),
]


class TestPerfVchildR2Depth:

    def setup_class(cls):
        apply_perf_flags()

        rpt.header("vchild R2: depth scaling")

        cls._db = "pf_vchild_r2_depth"
        cls._chain = build_vchild_chain(
            db=cls._db,
            depths=DEPTHS,
            rows_per_child=ROWS_PER_CHILD,
            data_cols=DATA_COLS,
            vgroups=1,
        )

        cls._results = {}
        for qname, qtpl in QUERIES:
            cls._results[qname] = {}
            for depth in DEPTHS:
                cls._results[qname][depth] = median(bench(qtpl.format(tbl=cls._chain[depth])))

    def test_01_depth_matrix(self):
        """Vchild R2 depth: L0-L32 depth x query matrix

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vchild

        Jira: None

        History:
            - 2026-04-20 Created
        """
        tdSql.execute(f"USE {self._db};")
        rpt.emit("\n=== Depth x Query Matrix ===")
        rpt.emit(f"  {'Depth':<8} {'Query':<14} {'Median(ms)':>12} {'vs L0':>10} {'Delta Prev':>12}")
        rpt.emit("  " + "-" * 62)

        for qname, _ in QUERIES:
            base_ms = None
            prev_ms = None
            for depth in DEPTHS:
                ms = self._results[qname][depth]
                if base_ms is None:
                    base_ms = ms
                vs_l0 = f"{ms / base_ms * 100:.0f}%" if base_ms > 0 else "-"
                delta = f"{ms - prev_ms:+.2f}" if prev_ms is not None else "-"
                rpt.emit(f"  L{depth:<7} {qname:<14} {ms:>12.2f} {vs_l0:>10} {delta:>12}")
                prev_ms = ms

    def test_02_marginal_cost(self):
        """Vchild R2 depth: marginal cost per layer

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vchild

        Jira: None

        History:
            - 2026-04-20 Created
        """
        tdSql.execute(f"USE {self._db};")
        rpt.emit("\n=== Marginal Cost Summary ===")
        rpt.emit(f"  {'Query':<14} {'L1-L0(ms)':>12} {'L2-L1(ms)':>12} {'Avg L2+(ms)':>12} {'Max L2+(ms)':>12}")
        rpt.emit("  " + "-" * 70)

        for qname, _ in QUERIES:
            values = [self._results[qname][d] for d in DEPTHS]
            deltas = [values[i] - values[i - 1] for i in range(1, len(values))]
            pure_deltas = deltas[1:]  # L2-L1 onward
            avg_l2p = sum(pure_deltas) / len(pure_deltas) if pure_deltas else 0
            max_l2p = max(pure_deltas) if pure_deltas else 0
            rpt.emit(f"  {qname:<14} {deltas[0]:>12.2f} {deltas[1]:>12.2f} {avg_l2p:>12.2f} {max_l2p:>12.2f}")

    def test_zz_summary(self):
        rpt.footer()
        tdLog.info(f"Report: {REPORT_FILE}")
