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
"""Performance benchmark: vstable-ref-vstable big data + many tables (C-R4).

Measures how vstable chain overhead scales with data volume.
children=200, rows=[10000,50000,100000], data_cols=5, tag_cols=3,
vgroups=4, depths=[1,2,4]. Includes cold vs warm comparison.

Each row count gets its own database to avoid metadata accumulation.

Report written to /tmp/perf_vstable_r4_big_data_report.txt
"""

import time
from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from vstable_perf_util import build_vstable_chain

REPORT_FILE = "/tmp/perf_vstable_r4_big_data_report.txt"
rpt = PerfReport(REPORT_FILE)

ROW_POINTS = [10000, 50000]
DEPTH_POINTS = [1, 2, 4]
CHAIN_DEPTHS = [0, 1, 2, 3, 4]
QUERIES = [
    ("Q2 COUNT", "SELECT COUNT(*) FROM {stb}"),
    ("Q4a WHERE data", "SELECT * FROM {stb} WHERE c0 >= 100"),
    ("Q4b WHERE tag", "SELECT * FROM {stb} WHERE t0 = 0"),
    ("Q5 GROUP BY", "SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
]


def _db_name(rows):
    return f"pf_vstable_r4_r{rows}"


class TestPerfVstableR4BigData:

    def setup_class(cls):
        apply_perf_flags()
        rpt.header("vstable R4: big data + many tables")
        rpt.emit(f"  children: 200, rows: {ROW_POINTS}")
        rpt.emit(f"  depths: {DEPTH_POINTS}, data_cols: 5, tag_cols: 3, vgroups: 4")

    def test_01_rows_depth_matrix(self):
        """Vstable R4 big data: rows x depth matrix

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Rows x Depth Matrix ===")

        for qname, qtpl in QUERIES:
            rpt.emit(f"\n  --- {qname} ---")
            rpt.emit(f"  {'Rows':<12} {'L1(ms)':>12} {'L2(ms)':>12} {'L4(ms)':>12} {'L2/L1':>10} {'L4/L1':>10}")
            rpt.emit("  " + "-" * 72)

            for rows in ROW_POINTS:
                db = _db_name(rows)
                chain = build_vstable_chain(
                    db=db,
                    depths=CHAIN_DEPTHS,
                    children=200,
                    rows_per_child=rows,
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
                rpt.emit(f"  {rows:<12} {times[1]:>12.2f} {times[2]:>12.2f} {times[4]:>12.2f} {l2_ratio:>10} {l4_ratio:>10}")
        rpt.flush()

    def test_02_overhead_vs_rows(self):
        """Vstable R4 big data: L4-L1 overhead vs rows

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Overhead (L4-L1) vs Row Count ===")
        header_cols = "  ".join(f"{qn + ' (ms)':>16}" for qn, _ in QUERIES)
        rpt.emit(f"  {'Rows':<12} {header_cols}")
        rpt.emit("  " + "-" * 80)

        for rows in ROW_POINTS:
            db = _db_name(rows)
            chain = build_vstable_chain(
                db=db,
                depths=CHAIN_DEPTHS,
                children=200,
                rows_per_child=rows,
                data_cols=5,
                tag_cols=3,
                vgroups=4,
                use_tag_ref=False,
            )
            overheads = []
            for qname, qtpl in QUERIES:
                t1 = median(bench(qtpl.format(stb=chain[1])))
                t4 = median(bench(qtpl.format(stb=chain[4])))
                overheads.append(f"{t4 - t1:>16.2f}")
            rpt.emit(f"  {rows:<12} {'  '.join(overheads)}")
        rpt.flush()

    def test_03_cold_vs_warm(self):
        """Vstable R4 big data: cold vs warm for rows=10000

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rows = 10000
        db = _db_name(rows)
        chain = build_vstable_chain(
            db=db,
            depths=CHAIN_DEPTHS,
            children=200,
            rows_per_child=rows,
            data_cols=5,
            tag_cols=3,
            vgroups=4,
            use_tag_ref=False,
        )
        rpt.emit("\n=== Cold vs Warm (rows=10000, children=200) ===")
        rpt.emit(f"  {'Depth':<8} {'Query':<18} {'Cold(ms)':>12} {'Warm(ms)':>12} {'Warm/Cold':>12}")
        rpt.emit("  " + "-" * 66)

        for depth in DEPTH_POINTS:
            for name, qtpl in [
                ("Q2 COUNT", "SELECT COUNT(*) FROM {stb}"),
                ("Q4b WHERE tag", "SELECT * FROM {stb} WHERE t0 = 0"),
                ("Q5 GROUP BY", "SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
            ]:
                sql = qtpl.format(stb=chain[depth])
                tdSql.execute(f"FLUSH DATABASE {db};")
                time.sleep(1)
                t0 = time.perf_counter()
                tdSql.query(sql)
                cold_ms = (time.perf_counter() - t0) * 1000.0
                warm_ms = median(bench(sql))
                ratio = f"{warm_ms / cold_ms * 100:.0f}%" if cold_ms > 0 else "-"
                rpt.emit(f"  L{depth:<7} {name:<18} {cold_ms:>12.2f} {warm_ms:>12.2f} {ratio:>12}")
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
