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
"""Performance benchmark: vstable-ref-vstable many col-refs (C-R5).

Measures how performance scales with the number of referenced columns.
data_cols=[5,10,50,100,200], children=50, rows=1000, tag_cols=3,
vgroups=2, depths=[1,2,4].

Each data_cols value gets its own database to avoid metadata accumulation.

Report written to /tmp/perf_vstable_r5_many_colrefs_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from vstable_perf_util import build_vstable_chain

REPORT_FILE = "/tmp/perf_vstable_r5_many_colrefs_report.txt"
rpt = PerfReport(REPORT_FILE)

WIDTH_POINTS = [5, 10, 50, 100, 200]
DEPTH_POINTS = [1, 2, 4]
CHAIN_DEPTHS = [0, 1, 2, 3, 4]
QUERIES = [
    ("Q1 SELECT *", "SELECT * FROM {stb}"),
    ("Q2 COUNT", "SELECT COUNT(*) FROM {stb}"),
    ("Q4 WHERE tag", "SELECT * FROM {stb} WHERE t0 = 0"),
]


def _db_name(data_cols):
    return f"pf_vstable_r5_w{data_cols}"


class TestPerfVstableR5ManyColrefs:

    def setup_class(cls):
        apply_perf_flags()
        rpt.header("vstable R5: many col-refs")
        rpt.emit(f"  data_cols: {WIDTH_POINTS}")
        rpt.emit(f"  children: 50, rows: 1000, tag_cols: 3, vgroups: 2")
        rpt.emit(f"  depths: {DEPTH_POINTS}")

    def test_01_width_depth_matrix(self):
        """Vstable R5 many col-refs: width x depth matrix

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Width x Depth Matrix ===")

        for qname, qtpl in QUERIES:
            rpt.emit(f"\n  --- {qname} ---")
            rpt.emit(f"  {'Width':<10} {'L1(ms)':>12} {'L2(ms)':>12} {'L4(ms)':>12} {'L2/L1':>10} {'L4/L1':>10}")
            rpt.emit("  " + "-" * 70)

            for data_cols in WIDTH_POINTS:
                db = _db_name(data_cols)
                chain = build_vstable_chain(
                    db=db,
                    depths=CHAIN_DEPTHS,
                    children=50,
                    rows_per_child=1000,
                    data_cols=data_cols,
                    tag_cols=3,
                    vgroups=2,
                    use_tag_ref=False,
                )
                times = {}
                for d in DEPTH_POINTS:
                    times[d] = median(bench(qtpl.format(stb=chain[d])))
                l2_ratio = f"{times[2] / times[1] * 100:.0f}%" if times[1] > 0 else "-"
                l4_ratio = f"{times[4] / times[1] * 100:.0f}%" if times[1] > 0 else "-"
                rpt.emit(f"  {data_cols:<10} {times[1]:>12.2f} {times[2]:>12.2f} {times[4]:>12.2f} {l2_ratio:>10} {l4_ratio:>10}")
        rpt.flush()

    def test_02_overhead_vs_width(self):
        """Vstable R5 many col-refs: L4-L1 overhead vs width

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Overhead (L4-L1) vs Width ===")
        header_cols = "  ".join(f"{qn + ' (ms)':>16}" for qn, _ in QUERIES)
        rpt.emit(f"  {'Width':<10} {header_cols}")
        rpt.emit("  " + "-" * 70)

        for data_cols in WIDTH_POINTS:
            db = _db_name(data_cols)
            chain = build_vstable_chain(
                db=db,
                depths=CHAIN_DEPTHS,
                children=50,
                rows_per_child=1000,
                data_cols=data_cols,
                tag_cols=3,
                vgroups=2,
                use_tag_ref=False,
            )
            overheads = []
            for qname, qtpl in QUERIES:
                t1 = median(bench(qtpl.format(stb=chain[1])))
                t4 = median(bench(qtpl.format(stb=chain[4])))
                overheads.append(f"{t4 - t1:>16.2f}")
            rpt.emit(f"  {data_cols:<10} {'  '.join(overheads)}")
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
