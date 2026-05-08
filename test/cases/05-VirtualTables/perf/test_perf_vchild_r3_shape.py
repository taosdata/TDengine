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
"""Performance benchmark: vchild-ref-vchild query shape sensitivity (B-R3).

Tests whether different query types are sensitive to chain depth.
Uses depths=[1,4,16,32] with a full query set.

Report written to /tmp/perf_vchild_r3_shape_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from vchild_perf_util import build_vchild_chain, half_filter_value

CHAIN_DEPTHS = [0, 1, 2, 4, 8, 16, 32]
DEPTH_POINTS = [1, 4, 16, 32]
ROWS_PER_CHILD = 1000
DATA_COLS = 5
REPORT_FILE = "/tmp/perf_vchild_r3_shape_report.txt"

rpt = PerfReport(REPORT_FILE)

FILTER_VAL = half_filter_value(ROWS_PER_CHILD)
SHAPES = [
    ("SELECT *", "SELECT * FROM {tbl}"),
    ("projection", "SELECT c0, c1 FROM {tbl} LIMIT 100"),
    ("COUNT+SUM", "SELECT COUNT(*), SUM(c0) FROM {tbl}"),
    ("data filter", f"SELECT * FROM {{tbl}} WHERE c0 >= {FILTER_VAL}"),
    ("tag filter", "SELECT * FROM {tbl} WHERE gid = 11 AND region = 'alpha'"),
    ("TAG+DATA", f"SELECT * FROM {{tbl}} WHERE gid = 11 AND region = 'alpha' AND c1 >= {FILTER_VAL + 100}"),
    ("ORDER BY", "SELECT c0, c1 FROM {tbl} ORDER BY c0 DESC LIMIT 100"),
    ("LAST", "SELECT LAST(c0) FROM {tbl}"),
]


class TestPerfVchildR3Shape:

    def setup_class(cls):
        apply_perf_flags()

        rpt.header("vchild R3: query shape sensitivity")

        cls._db = "pf_vchild_r3_shape"
        cls._chain = build_vchild_chain(
            db=cls._db,
            depths=CHAIN_DEPTHS,
            rows_per_child=ROWS_PER_CHILD,
            data_cols=DATA_COLS,
            vgroups=1,
        )

        cls._results = {}
        for name, qtpl in SHAPES:
            cls._results[name] = {}
            for depth in DEPTH_POINTS:
                cls._results[name][depth] = median(bench(qtpl.format(tbl=cls._chain[depth])))

    def test_01_query_shapes(self):
        """Vchild R3 shape: L1/L4/L16/L32 query-shape comparison

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
        rpt.emit("\n=== Query Shapes ===")
        rpt.emit(f"  {'Shape':<18} {'L1(ms)':>10} {'L4(ms)':>10} {'L16(ms)':>10} {'L32(ms)':>10} {'L32/L1':>10}")
        rpt.emit("  " + "-" * 76)

        for name, _ in SHAPES:
            l1 = self._results[name][1]
            l4 = self._results[name][4]
            l16 = self._results[name][16]
            l32 = self._results[name][32]
            ratio = f"{l32 / l1 * 100:.0f}%" if l1 > 0 else "-"
            rpt.emit(f"  {name:<18} {l1:>10.2f} {l4:>10.2f} {l16:>10.2f} {l32:>10.2f} {ratio:>10}")

    def test_02_hotspots(self):
        """Vchild R3 shape: hotspot summary (shapes ranked by L32/L1 ratio)

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
        rpt.emit("\n=== Hotspots (sorted by L32/L1) ===")
        rpt.emit(f"  {'Shape':<18} {'L1(ms)':>10} {'L32(ms)':>10} {'Delta(ms)':>10} {'L32/L1':>10}")
        rpt.emit("  " + "-" * 66)

        hotspots = []
        for name, _ in SHAPES:
            l1 = self._results[name][1]
            l32 = self._results[name][32]
            ratio = l32 / l1 if l1 > 0 else 0
            hotspots.append((ratio, name, l1, l32))

        for _, name, l1, l32 in sorted(hotspots, reverse=True):
            ratio_str = f"{l32 / l1 * 100:.0f}%" if l1 > 0 else "-"
            rpt.emit(f"  {name:<18} {l1:>10.2f} {l32:>10.2f} {l32 - l1:>10.2f} {ratio_str:>10}")

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
