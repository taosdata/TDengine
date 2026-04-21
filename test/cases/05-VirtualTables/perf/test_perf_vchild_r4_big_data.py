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
"""Performance benchmark: vchild-ref-vchild big data + deep chain (B-R4).

Tests rows=[1K,10K,50K,100K] x depths=[1,8,32] with cold vs warm
measurement for each combination.

Report written to /tmp/perf_vchild_r4_big_data_report.txt
"""

import time
from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from vchild_perf_util import build_vchild_chain, half_filter_value

ROW_POINTS = [1000, 10000, 50000]
DEPTH_POINTS = [1, 8, 32]
DATA_COLS = 5
REPORT_FILE = "/tmp/perf_vchild_r4_big_data_report.txt"

rpt = PerfReport(REPORT_FILE)


class TestPerfVchildR4BigData:

    def setup_class(cls):
        apply_perf_flags()

        rpt.header("vchild R4: big data + deep chain")

        # Build one chain per row count; each gets its own database
        cls._chains = {}
        cls._warm = {}  # (rows, depth, qname) -> median ms (warm = 5th run)
        cls._cold = {}  # (rows, depth, qname) -> first-run ms

        for rows in ROW_POINTS:
            db = f"pf_vchild_r4_bd_{rows}"
            chain = build_vchild_chain(
                db=db,
                depths=[0] + DEPTH_POINTS,
                rows_per_child=rows,
                data_cols=DATA_COLS,
                vgroups=1,
            )
            cls._chains[rows] = chain

            filter_val = half_filter_value(rows)
            queries = [
                ("COUNT", "SELECT COUNT(*) FROM {tbl}"),
                ("data filter", f"SELECT * FROM {{tbl}} WHERE c0 >= {filter_val}"),
                ("LAST", "SELECT LAST(c0) FROM {tbl}"),
            ]

            for depth in DEPTH_POINTS:
                for qname, qtpl in queries:
                    sql = qtpl.format(tbl=chain[depth])
                    # Cold run
                    tdSql.execute(f"FLUSH DATABASE {db};")
                    time.sleep(0.5)
                    t0 = time.perf_counter()
                    tdSql.query(sql)
                    cold_ms = (time.perf_counter() - t0) * 1000.0
                    # Warm runs (5 repeats, take median)
                    warm_ms = median(bench(sql))
                    cls._cold[(rows, depth, qname)] = cold_ms
                    cls._warm[(rows, depth, qname)] = warm_ms

    def test_01_rows_depth_matrix(self):
        """Vchild R4 big data: rows x depth matrix (warm median)

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vchild

        Jira: None

        History:
            - 2026-04-20 Created
        """
        for qname in ["COUNT", "data filter", "LAST"]:
            rpt.emit(f"\n=== {qname}: Rows x Depth (warm) ===")
            rpt.emit(f"  {'Rows':<10} {'L1(ms)':>12} {'L8(ms)':>12} {'L32(ms)':>12} {'L8/L1':>10} {'L32/L1':>10}")
            rpt.emit("  " + "-" * 70)

            for rows in ROW_POINTS:
                l1 = self._warm[(rows, 1, qname)]
                l8 = self._warm[(rows, 8, qname)]
                l32 = self._warm[(rows, 32, qname)]
                r8 = f"{l8 / l1 * 100:.0f}%" if l1 > 0 else "-"
                r32 = f"{l32 / l1 * 100:.0f}%" if l1 > 0 else "-"
                rpt.emit(f"  {rows:<10} {l1:>12.2f} {l8:>12.2f} {l32:>12.2f} {r8:>10} {r32:>10}")

    def test_02_cold_vs_warm(self):
        """Vchild R4 big data: cold vs warm comparison

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vchild

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Cold vs Warm ===")
        rpt.emit(f"  {'Rows':<10} {'Depth':<8} {'Query':<14} {'Cold(ms)':>12} {'Warm(ms)':>12} {'Warm/Cold':>12}")
        rpt.emit("  " + "-" * 76)

        for rows in ROW_POINTS:
            for depth in DEPTH_POINTS:
                for qname in ["COUNT", "data filter", "LAST"]:
                    cold = self._cold[(rows, depth, qname)]
                    warm = self._warm[(rows, depth, qname)]
                    ratio = f"{warm / cold * 100:.0f}%" if cold > 0 else "-"
                    rpt.emit(f"  {rows:<10} L{depth:<7} {qname:<14} {cold:>12.2f} {warm:>12.2f} {ratio:>12}")

    def test_zz_summary(self):
        rpt.footer()
        tdLog.info(f"Report: {REPORT_FILE}")
