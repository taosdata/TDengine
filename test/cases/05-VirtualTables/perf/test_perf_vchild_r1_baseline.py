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
"""Performance benchmark: vchild-ref-vchild baseline (B-R1).

Compares L0 (physical source), L1 (first virtual hop), and L2 (first pure
vchild-ref-vchild hop) across a standard query set.

Report written to /tmp/perf_vchild_r1_baseline_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from vchild_perf_util import build_vchild_chain, half_filter_value

ROWS_PER_CHILD = 10000
DATA_COLS = 5
DEPTHS = [0, 1, 2]
REPORT_FILE = "/tmp/perf_vchild_r1_baseline_report.txt"

rpt = PerfReport(REPORT_FILE)

FILTER_VAL = half_filter_value(ROWS_PER_CHILD)
QUERIES = [
    ("SELECT *", "SELECT * FROM {tbl}"),
    ("COUNT(*)", "SELECT COUNT(*) FROM {tbl}"),
    ("SUM+AVG", "SELECT SUM(c0), AVG(c1) FROM {tbl}"),
    ("WHERE data", f"SELECT * FROM {{tbl}} WHERE c0 >= {FILTER_VAL}"),
    ("WHERE tag", "SELECT * FROM {tbl} WHERE gid = 11 AND region = 'alpha'"),
    ("ORDER BY", "SELECT c0, c1 FROM {tbl} ORDER BY c0 DESC LIMIT 1000"),
    ("LAST", "SELECT LAST(c0) FROM {tbl}"),
]


class TestPerfVchildR1Baseline:

    def setup_class(cls):
        apply_perf_flags()

        rpt.header("vchild R1: baseline")

        cls._db = "pf_vchild_r1_baseline"
        cls._chain = build_vchild_chain(
            db=cls._db,
            depths=DEPTHS,
            rows_per_child=ROWS_PER_CHILD,
            data_cols=DATA_COLS,
            vgroups=1,
        )

        # Pre-run all queries to populate results
        cls._results = {}
        for qname, qtpl in QUERIES:
            cls._results[qname] = {}
            for depth in DEPTHS:
                cls._results[qname][depth] = median(bench(qtpl.format(tbl=cls._chain[depth])))

    def test_01_query_matrix(self):
        """Vchild R1 baseline: L0 vs L1 vs L2 query matrix

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vchild

        Jira: None

        History:
            - 2026-04-20 Created
        """
        tdSql.execute(f"USE {self._db};")
        rpt.emit("\n=== Query Matrix ===")
        rpt.emit(f"  {'Depth':<14} {'Query':<14} {'Median(ms)':>12} {'vs L0':>10} {'Delta Prev':>12}")
        rpt.emit("  " + "-" * 68)

        depth_names = {0: "L0 source", 1: "L1 bootstrap", 2: "L2 pure"}
        for qname, _ in QUERIES:
            base_ms = None
            prev_ms = None
            for depth in DEPTHS:
                ms = self._results[qname][depth]
                if base_ms is None:
                    base_ms = ms
                vs_l0 = f"{ms / base_ms * 100:.0f}%" if base_ms > 0 else "-"
                delta = f"{ms - prev_ms:+.2f}" if prev_ms is not None else "-"
                rpt.emit(f"  {depth_names[depth]:<14} {qname:<14} {ms:>12.2f} {vs_l0:>10} {delta:>12}")
                prev_ms = ms

    def test_02_entry_cost_summary(self):
        """Vchild R1 baseline: entry-cost summary (deltas)

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vchild

        Jira: None

        History:
            - 2026-04-20 Created
        """
        tdSql.execute(f"USE {self._db};")
        rpt.emit("\n=== Entry Cost Summary ===")
        rpt.emit(f"  {'Query':<14} {'L1-L0(ms)':>12} {'L2-L1(ms)':>12} {'L2-L0(ms)':>12}")
        rpt.emit("  " + "-" * 56)

        for qname, _ in QUERIES:
            l0 = self._results[qname][0]
            l1 = self._results[qname][1]
            l2 = self._results[qname][2]
            rpt.emit(f"  {qname:<14} {l1 - l0:>12.2f} {l2 - l1:>12.2f} {l2 - l0:>12.2f}")

    def test_zz_summary(self):
        rpt.footer()
        tdLog.info(f"Report: {REPORT_FILE}")
