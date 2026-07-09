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
"""Performance benchmark: vchild-ref-vchild many col-refs (B-R5).

Tests data_cols=[1,5,10,50,100,200] x depths=[1,8] to measure how
column reference width interacts with chain depth.

Report written to /tmp/perf_vchild_r5_many_colrefs_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from vchild_perf_util import build_vchild_chain, half_filter_value

WIDTHS = [1, 5, 10, 50, 100, 200]
DEPTH_POINTS = [1, 8]
ROWS_PER_CHILD = 10000
REPORT_FILE = "/tmp/perf_vchild_r5_many_colrefs_report.txt"

rpt = PerfReport(REPORT_FILE)


class TestPerfVchildR5ManyColrefs:

    def setup_class(cls):
        apply_perf_flags()

        rpt.header("vchild R5: many col-refs")

        cls._results = {}

        for width in WIDTHS:
            db = f"pf_vchild_r5_w{width}"
            chain = build_vchild_chain(
                db=db,
                depths=[0] + DEPTH_POINTS,
                rows_per_child=ROWS_PER_CHILD,
                data_cols=width,
                vgroups=1,
            )

            filter_val = half_filter_value(ROWS_PER_CHILD)
            queries = [
                ("SELECT *", f"SELECT * FROM {{tbl}}"),
                ("COUNT", f"SELECT COUNT(*) FROM {{tbl}}"),
                ("data filter", f"SELECT * FROM {{tbl}} WHERE c0 >= {filter_val}"),
            ]

            for qname, qtpl in queries:
                cls._results.setdefault(qname, {})
                cls._results[qname].setdefault(width, {})
                for depth in DEPTH_POINTS:
                    sql = qtpl.format(tbl=chain[depth])
                    cls._results[qname][width][depth] = median(bench(sql))

    def test_01_width_by_query(self):
        """Vchild R5 col-refs: width x depth matrix per query

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vchild

        Jira: None

        History:
            - 2026-04-20 Created
        """
        for qname in ["SELECT *", "COUNT", "data filter"]:
            rpt.emit(f"\n=== {qname} ===")
            rpt.emit(f"  {'Ref Cols':<10} {'L1(ms)':>12} {'L8(ms)':>12} {'L8/L1':>10}")
            rpt.emit("  " + "-" * 50)

            for width in WIDTHS:
                l1 = self._results[qname][width][1]
                l8 = self._results[qname][width][8]
                ratio = f"{l8 / l1 * 100:.0f}%" if l1 > 0 else "-"
                rpt.emit(f"  {width:<10} {l1:>12.2f} {l8:>12.2f} {ratio:>10}")

    def test_02_width_sensitivity(self):
        """Vchild R5 col-refs: width-sensitivity summary

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vchild

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Width Sensitivity Summary ===")
        rpt.emit(f"  {'Query':<14} {'W1-L1(ms)':>12} {'W200-L8(ms)':>14} {'W200-L8/W1-L1':>16}")
        rpt.emit("  " + "-" * 62)

        for qname in ["SELECT *", "COUNT", "data filter"]:
            base = self._results[qname][1][1]
            high = self._results[qname][200][8]
            ratio = f"{high / base * 100:.0f}%" if base > 0 else "-"
            rpt.emit(f"  {qname:<14} {base:>12.2f} {high:>14.2f} {ratio:>16}")

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
