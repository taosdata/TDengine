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
"""Performance benchmark: vchild-ref-vchild small data + deep chain (B-R7).

Minimal data (100 rows) to isolate pure chain resolution overhead from
I/O noise. Tests depths=[1,2,4,8,16,32].

Report written to /tmp/perf_vchild_r7_small_data_deep_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from vchild_perf_util import build_vchild_chain

DEPTHS = [0, 1, 2, 4, 8, 16, 32]
ROWS_PER_CHILD = 100
DATA_COLS = 5
REPORT_FILE = "/tmp/perf_vchild_r7_small_data_deep_report.txt"

rpt = PerfReport(REPORT_FILE)

QUERIES = [
    ("SELECT *", "SELECT * FROM {tbl}"),
    ("COUNT", "SELECT COUNT(*) FROM {tbl}"),
    ("LAST", "SELECT LAST(c0) FROM {tbl}"),
]


class TestPerfVchildR7SmallDataDeep:

    def setup_class(cls):
        apply_perf_flags()

        rpt.header("vchild R7: small data + deep chain")

        cls._db = "pf_vchild_r7_small"
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
        """Vchild R7 small data: L0-L32 depth matrix

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vchild

        Jira: None

        History:
            - 2026-04-20 Created
        """
        tdSql.execute(f"USE {self._db};")
        rpt.emit("\n=== Depth Matrix ===")
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

    def test_02_chain_resolution_overhead(self):
        """Vchild R7 small data: chain resolution overhead summary

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vchild

        Jira: None

        History:
            - 2026-04-20 Created
        """
        tdSql.execute(f"USE {self._db};")
        rpt.emit("\n=== Chain Resolution Overhead ===")
        rpt.emit(f"  {'Query':<14} {'L1-L0(ms)':>12} {'L32-L1(ms)':>12} {'Avg delta(ms)':>14} {'L32/L1':>10}")
        rpt.emit("  " + "-" * 68)

        for qname, _ in QUERIES:
            values = [self._results[qname][d] for d in DEPTHS]
            l1_l0 = values[1] - values[0] if len(values) > 1 else 0
            l32_l1 = values[-1] - values[1] if len(values) > 2 else 0
            deltas = [values[i] - values[i - 1] for i in range(2, len(values))]
            avg_delta = sum(deltas) / len(deltas) if deltas else 0
            l32_l1_ratio = f"{values[-1] / values[1] * 100:.0f}%" if values[1] > 0 else "-"
            rpt.emit(f"  {qname:<14} {l1_l0:>12.2f} {l32_l1:>12.2f} {avg_delta:>14.2f} {l32_l1_ratio:>10}")

    def test_zz_summary(self):
        """Write final report summary

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vchild

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.footer()
        tdLog.info(f"Report: {REPORT_FILE}")
