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
"""Performance benchmark: vstable-ref-vstable cross-db (C-R6).

Compares same-database vs cross-database vstable chains.
children=20, rows=1000, data_cols=5, tag_cols=3, vgroups=2, depths=[1,2,4].

Same-db: all layers in pf_vstable_samedb.
Cross-db: L0 in pf_vstable_xdb0, L1 in pf_vstable_xdb1, etc.

Report written to /tmp/perf_vstable_r6_cross_db_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from vstable_perf_util import build_vstable_chain, build_vstable_chain_cross_db

REPORT_FILE = "/tmp/perf_vstable_r6_cross_db_report.txt"
rpt = PerfReport(REPORT_FILE)

DEPTH_POINTS = [1, 2, 4]
CHAIN_DEPTHS = [0, 1, 2, 3, 4]
QUERIES = [
    ("Q1 SELECT *", "SELECT * FROM {stb}"),
    ("Q2 COUNT", "SELECT COUNT(*) FROM {stb}"),
    ("Q4 WHERE tag", "SELECT * FROM {stb} WHERE t0 = 0"),
    ("Q5 GROUP BY", "SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
]

CROSS_DBS = [
    "pf_vstable_xdb0",
    "pf_vstable_xdb1",
    "pf_vstable_xdb2",
    "pf_vstable_xdb3",
    "pf_vstable_xdb4",
]


class TestPerfVstableR6CrossDb:

    def setup_class(cls):
        apply_perf_flags()
        rpt.header("vstable R6: cross-db comparison")
        rpt.emit(f"  children: 20, rows: 1000, data_cols: 5, tag_cols: 3, vgroups: 2")
        rpt.emit(f"  depths: {DEPTH_POINTS}")

        # Build same-db chain
        cls._same_chain = build_vstable_chain(
            db="pf_vstable_samedb",
            depths=CHAIN_DEPTHS,
            children=20,
            rows_per_child=1000,
            data_cols=5,
            tag_cols=3,
            vgroups=2,
            use_tag_ref=False,
        )

        # Build cross-db chain
        cls._cross_chain = build_vstable_chain_cross_db(
            dbs=CROSS_DBS[:],
            depths=CHAIN_DEPTHS,
            children=20,
            rows_per_child=1000,
            data_cols=5,
            tag_cols=3,
            vgroups=2,
        )

        # Pre-run all queries for both chains
        cls._same_results = {}
        cls._cross_results = {}
        for qname, qtpl in QUERIES:
            cls._same_results[qname] = {}
            cls._cross_results[qname] = {}
            for d in DEPTH_POINTS:
                cls._same_results[qname][d] = median(bench(qtpl.format(stb=cls._same_chain[d])))
                cls._cross_results[qname][d] = median(bench(qtpl.format(stb=cls._cross_chain[d])))

    def test_01_same_vs_cross(self):
        """Vstable R6 cross-db: same-db vs cross-db comparison

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Same-DB vs Cross-DB ===")
        rpt.emit(f"  {'Query':<18} {'Depth':<8} {'Same(ms)':>12} {'Cross(ms)':>12} {'Delta(ms)':>12} {'Cross/Same':>12}")
        rpt.emit("  " + "-" * 78)

        for qname, _ in QUERIES:
            for d in DEPTH_POINTS:
                same_ms = self._same_results[qname][d]
                cross_ms = self._cross_results[qname][d]
                delta = cross_ms - same_ms
                ratio = f"{cross_ms / same_ms * 100:.0f}%" if same_ms > 0 else "-"
                rpt.emit(f"  {qname:<18} L{d:<7} {same_ms:>12.2f} {cross_ms:>12.2f} {delta:>12.2f} {ratio:>12}")
            rpt.emit("  " + "-" * 78)
        rpt.flush()

    def test_02_cross_db_overhead_summary(self):
        """Vstable R6 cross-db: overhead summary by depth

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Cross-DB Overhead Summary ===")
        rpt.emit(f"  {'Depth':<8} {'Q1 Delta(ms)':>16} {'Q2 Delta(ms)':>16} {'Q4 Delta(ms)':>16} {'Q5 Delta(ms)':>16}")
        rpt.emit("  " + "-" * 80)

        for d in DEPTH_POINTS:
            deltas = []
            for qname, _ in QUERIES:
                same_ms = self._same_results[qname][d]
                cross_ms = self._cross_results[qname][d]
                deltas.append(f"{cross_ms - same_ms:>16.2f}")
            rpt.emit(f"  L{d:<7} {'  '.join(deltas)}")
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
