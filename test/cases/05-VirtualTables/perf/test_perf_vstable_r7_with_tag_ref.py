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
"""Performance benchmark: vstable-ref-vstable with tag-ref (C-R7).

Compares three vstable chain variants:
  1. Literal-tag chain  (use_tag_ref=False) - baseline
  2. Tag-ref chain      (use_tag_ref=True)  - tags reference source table
  3. L0 physical table                      - ground truth

children=20, rows=1000, data_cols=5, tag_cols=3, vgroups=2, depths=[1,2,4].

Report written to /tmp/perf_vstable_r7_with_tag_ref_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from vstable_perf_util import build_vstable_chain

REPORT_FILE = "/tmp/perf_vstable_r7_with_tag_ref_report.txt"
rpt = PerfReport(REPORT_FILE)

DEPTH_POINTS = [1, 2, 4]
CHAIN_DEPTHS = [0, 1, 2, 3, 4]
QUERIES = [
    ("Q2 COUNT", "SELECT COUNT(*) FROM {stb}"),
    ("Q4 WHERE tag", "SELECT * FROM {stb} WHERE t0 = 0"),
    ("Q5 GROUP BY", "SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
]


class TestPerfVstableR7WithTagRef:

    def setup_class(cls):
        apply_perf_flags()
        rpt.header("vstable R7: mixed tag-ref comparison")
        rpt.emit(f"  children: 20, rows: 1000, data_cols: 5, tag_cols: 3, vgroups: 2")
        rpt.emit(f"  depths: {DEPTH_POINTS}")
        rpt.emit(f"  variants: literal-tag, tag-ref, L0-physical")

        # Build literal-tag chain (baseline)
        cls._literal_chain = build_vstable_chain(
            db="pf_vstable_r7_literal",
            depths=CHAIN_DEPTHS,
            children=20,
            rows_per_child=1000,
            data_cols=5,
            tag_cols=3,
            vgroups=2,
            use_tag_ref=False,
        )

        # Build tag-ref chain
        cls._tagref_chain = build_vstable_chain(
            db="pf_vstable_r7_tagref",
            depths=CHAIN_DEPTHS,
            children=20,
            rows_per_child=1000,
            data_cols=5,
            tag_cols=3,
            vgroups=2,
            use_tag_ref=True,
        )

        # Pre-run all queries for both chains
        cls._literal_results = {}
        cls._tagref_results = {}
        for qname, qtpl in QUERIES:
            cls._literal_results[qname] = {}
            cls._tagref_results[qname] = {}
            for d in DEPTH_POINTS:
                cls._literal_results[qname][d] = median(bench(qtpl.format(stb=cls._literal_chain[d])))
                cls._tagref_results[qname][d] = median(bench(qtpl.format(stb=cls._tagref_chain[d])))
            # L0 physical (same for both chains, just use literal L0)
            cls._literal_results[qname][0] = median(bench(qtpl.format(stb=cls._literal_chain[0])))

    def test_01_literal_vs_tagref(self):
        """Vstable R7 tag-ref: literal-tag vs tag-ref vs L0-physical

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Literal-Tag vs Tag-Ref Comparison ===")
        rpt.emit(f"  {'Query':<18} {'Depth':<8} {'L0(ms)':>10} {'Literal(ms)':>14} {'TagRef(ms)':>14} {'TR-Lit(ms)':>12} {'TR/Lit':>10}")
        rpt.emit("  " + "-" * 82)

        for qname, _ in QUERIES:
            l0_ms = self._literal_results[qname][0]
            for d in DEPTH_POINTS:
                lit_ms = self._literal_results[qname][d]
                tr_ms = self._tagref_results[qname][d]
                delta = tr_ms - lit_ms
                ratio = f"{tr_ms / lit_ms * 100:.0f}%" if lit_ms > 0 else "-"
                rpt.emit(f"  {qname:<18} L{d:<7} {l0_ms:>10.2f} {lit_ms:>14.2f} {tr_ms:>14.2f} {delta:>12.2f} {ratio:>10}")
            rpt.emit("  " + "-" * 82)
        rpt.flush()

    def test_02_tagref_overhead_by_depth(self):
        """Vstable R7 tag-ref: tag-ref overhead by depth

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Tag-Ref Overhead by Depth ===")
        rpt.emit(f"  {'Depth':<8} {'Q2 TR-Lit(ms)':>16} {'Q4 TR-Lit(ms)':>16} {'Q5 TR-Lit(ms)':>16}")
        rpt.emit("  " + "-" * 64)

        for d in DEPTH_POINTS:
            deltas = []
            for qname, _ in QUERIES:
                lit_ms = self._literal_results[qname][d]
                tr_ms = self._tagref_results[qname][d]
                deltas.append(f"{tr_ms - lit_ms:>16.2f}")
            rpt.emit(f"  L{d:<7} {'  '.join(deltas)}")
        rpt.flush()

    def test_03_chain_overhead_summary(self):
        """Vstable R7 tag-ref: chain depth overhead (literal baseline)

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vstable

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Chain Depth Overhead (Literal-Tag Baseline) ===")
        rpt.emit(f"  {'Query':<18} {'L1-L0(ms)':>12} {'L2-L1(ms)':>12} {'L4-L1(ms)':>12}")
        rpt.emit("  " + "-" * 60)

        for qname, _ in QUERIES:
            l0 = self._literal_results[qname][0]
            l1 = self._literal_results[qname][1]
            l2 = self._literal_results[qname][2]
            l4 = self._literal_results[qname][4]
            rpt.emit(f"  {qname:<18} {l1 - l0:>12.2f} {l2 - l1:>12.2f} {l4 - l1:>12.2f}")
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
