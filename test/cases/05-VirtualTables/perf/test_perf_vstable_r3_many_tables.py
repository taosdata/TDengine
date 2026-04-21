###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""Performance benchmark: vstable-ref-vstable many tables, little data (C-R3).

Measures how fan-out overhead scales with children count when data is small.
children=[50,100,200,500], rows=100, data_cols=5, tag_cols=3, vgroups=4,
depths=[1,2,4].

Each children count gets its own database to avoid metadata accumulation.

Report written to /tmp/perf_vstable_r3_many_tables_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from vstable_perf_util import build_vstable_chain

REPORT_FILE = "/tmp/perf_vstable_r3_many_tables_report.txt"
rpt = PerfReport(REPORT_FILE)

CHILDREN_POINTS = [50, 100, 200, 500]
DEPTH_POINTS = [1, 2, 4]
CHAIN_DEPTHS = [0, 1, 2, 3, 4]
QUERIES = [
    ("Q2 COUNT", "SELECT COUNT(*) FROM {stb}"),
    ("Q4 WHERE tag", "SELECT * FROM {stb} WHERE t0 = 0"),
    ("Q5 GROUP BY", "SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
]


def _db_name(children):
    return f"pf_vstable_r3_c{children}"


class TestPerfVstableR3ManyTables:

    def setup_class(cls):
        apply_perf_flags()
        rpt.header("vstable R3: many tables, little data")
        rpt.emit(f"  children: {CHILDREN_POINTS}")
        rpt.emit(f"  depths: {DEPTH_POINTS}")
        rpt.emit(f"  rows/child: 100, data_cols: 5, tag_cols: 3, vgroups: 4")

    def test_01_children_scaling(self):
        """Vstable R3 many tables: children scaling with depth

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Children Scaling (rows=100, depths 1/2/4) ===")

        for qname, qtpl in QUERIES:
            rpt.emit(f"\n  --- {qname} ---")
            rpt.emit(f"  {'Children':<10} {'L1(ms)':>12} {'L2(ms)':>12} {'L4(ms)':>12} {'L2/L1':>10} {'L4/L1':>10} {'L4-L1(ms)':>12}")
            rpt.emit("  " + "-" * 80)

            for children in CHILDREN_POINTS:
                db = _db_name(children)
                chain = build_vstable_chain(
                    db=db,
                    depths=CHAIN_DEPTHS,
                    children=children,
                    rows_per_child=100,
                    data_cols=5,
                    tag_cols=3,
                    vgroups=4,
                    use_tag_ref=False,
                )
                times = {}
                for d in DEPTH_POINTS:
                    times[d] = median(bench(qtpl.format(stb=chain[d])))
                l2_ratio = f"{times[2] / times[1] * 100:.0f}%" if times[1] > 0 else "-"
                l4_ratio = f"{times[4] / times[1] * 100:.0f}%" if times[1] > 0 else "-"
                delta = f"{times[4] - times[1]:+.2f}"
                rpt.emit(f"  {children:<10} {times[1]:>12.2f} {times[2]:>12.2f} {times[4]:>12.2f} {l2_ratio:>10} {l4_ratio:>10} {delta:>12}")
        rpt.flush()

    def test_02_overhead_vs_children(self):
        """Vstable R3 many tables: L4-L1 overhead vs children count

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Overhead (L4-L1) vs Children Count ===")
        rpt.emit(f"  {'Children':<10} {'Q2 L4-L1(ms)':>16} {'Q4 L4-L1(ms)':>16} {'Q5 L4-L1(ms)':>16}")
        rpt.emit("  " + "-" * 64)

        for children in CHILDREN_POINTS:
            db = _db_name(children)
            chain = build_vstable_chain(
                db=db,
                depths=CHAIN_DEPTHS,
                children=children,
                rows_per_child=100,
                data_cols=5,
                tag_cols=3,
                vgroups=4,
                use_tag_ref=False,
            )
            overheads = []
            for qname, qtpl in QUERIES:
                t1 = median(bench(qtpl.format(stb=chain[1])))
                t4 = median(bench(qtpl.format(stb=chain[4])))
                overheads.append(t4 - t1)
            rpt.emit(f"  {children:<10} {overheads[0]:>16.2f} {overheads[1]:>16.2f} {overheads[2]:>16.2f}")
        rpt.flush()

    def test_zz_summary(self):
        """Write final report summary

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
