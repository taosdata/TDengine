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
"""Performance benchmark: vchild-ref-vchild cross-db (B-R6).

Compares same-db chains vs cross-db chains to isolate RPC overhead
from cross-database vchild references.

Report written to /tmp/perf_vchild_r6_cross_db_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from vchild_perf_util import build_vchild_chain, build_vchild_chain_cross_db, half_filter_value

ROWS_PER_CHILD = 10000
DATA_COLS = 5
DEPTHS = [1, 2, 4]
REPORT_FILE = "/tmp/perf_vchild_r6_cross_db_report.txt"

rpt = PerfReport(REPORT_FILE)

FILTER_VAL = half_filter_value(ROWS_PER_CHILD)
QUERIES = [
    ("SELECT *", "SELECT * FROM {tbl}"),
    ("COUNT", "SELECT COUNT(*) FROM {tbl}"),
    ("data filter", f"SELECT * FROM {{tbl}} WHERE c0 >= {FILTER_VAL}"),
    ("LAST", "SELECT LAST(c0) FROM {tbl}"),
]

CROSS_DBS = [
    "pf_vchild_xdb0",
    "pf_vchild_xdb1",
    "pf_vchild_xdb2",
    "pf_vchild_xdb3",
    "pf_vchild_xdb4",
]


class TestPerfVchildR6CrossDb:

    def setup_class(cls):
        apply_perf_flags()

        rpt.header("vchild R6: cross-db")

        # Same-db chain
        cls._samedb_chain = build_vchild_chain(
            db="pf_vchild_samedb",
            depths=DEPTHS,
            rows_per_child=ROWS_PER_CHILD,
            data_cols=DATA_COLS,
            vgroups=1,
        )

        # Cross-db chain
        cls._xdb_chain = build_vchild_chain_cross_db(
            dbs=CROSS_DBS,
            depths=DEPTHS,
            rows_per_child=ROWS_PER_CHILD,
            data_cols=DATA_COLS,
            vgroups=1,
        )

        # Pre-run queries
        cls._samedb_results = {}
        cls._xdb_results = {}
        for qname, qtpl in QUERIES:
            cls._samedb_results[qname] = {}
            cls._xdb_results[qname] = {}
            for depth in DEPTHS:
                cls._samedb_results[qname][depth] = median(
                    bench(qtpl.format(tbl=cls._samedb_chain[depth]))
                )
                cls._xdb_results[qname][depth] = median(
                    bench(qtpl.format(tbl=cls._xdb_chain[depth]))
                )

    def test_01_same_vs_cross(self):
        """Vchild R6 cross-db: same-db vs cross-db comparison

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vchild

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Same-db vs Cross-db ===")
        rpt.emit(f"  {'Mode':<12} {'Depth':<8} {'Query':<14} {'Median(ms)':>12} {'Cross/Same':>12}")
        rpt.emit("  " + "-" * 66)

        for qname, _ in QUERIES:
            for depth in DEPTHS:
                same_ms = self._samedb_results[qname][depth]
                xdb_ms = self._xdb_results[qname][depth]
                ratio = f"{xdb_ms / same_ms * 100:.0f}%" if same_ms > 0 else "-"
                rpt.emit(f"  {'same-db':<12} L{depth:<7} {qname:<14} {same_ms:>12.2f} {'100%':>12}")
                rpt.emit(f"  {'cross-db':<12} L{depth:<7} {qname:<14} {xdb_ms:>12.2f} {ratio:>12}")

    def test_02_overhead_summary(self):
        """Vchild R6 cross-db: cross-db overhead summary

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vchild

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Cross-db Overhead Summary ===")
        rpt.emit(f"  {'Depth':<8} {'Query':<14} {'Same(ms)':>12} {'Cross(ms)':>12} {'Delta(ms)':>12} {'Overhead':>10}")
        rpt.emit("  " + "-" * 76)

        for qname, _ in QUERIES:
            for depth in DEPTHS:
                same_ms = self._samedb_results[qname][depth]
                xdb_ms = self._xdb_results[qname][depth]
                delta = xdb_ms - same_ms
                ratio = f"{xdb_ms / same_ms * 100:.0f}%" if same_ms > 0 else "-"
                rpt.emit(f"  L{depth:<7} {qname:<14} {same_ms:>12.2f} {xdb_ms:>12.2f} {delta:>12.2f} {ratio:>10}")

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
