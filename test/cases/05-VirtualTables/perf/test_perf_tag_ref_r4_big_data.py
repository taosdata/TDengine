###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies,
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""Performance benchmark: tag-ref R4 big data + many tables.

Measures if tag-ref overhead gets drowned out by data volume.
Fixed children=200, sweep rows/child=[1000, 10000, 50000, 100000].

Queries: Q2 (COUNT), Q4 (WHERE data), Q5 (WHERE tag), Q6 (GROUP BY tag)
Each row count gets its own database.

Report written to /tmp/perf_tag_ref_r4_big_data_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from tag_ref_perf_util import build_tag_ref_env

REPORT_FILE = "/tmp/perf_tag_ref_r4_big_data_report.txt"
rpt = PerfReport(REPORT_FILE)

CHILDREN = 100
ROWS_SET = [1000, 10000, 50000]
DATA_COLS = 5
TAG_COLS = 3
VGROUPS = 4

QUERIES = [
    ("Q2 COUNT",    "SELECT COUNT(*) FROM {stb}"),
    ("Q4 WHERE dt", "SELECT * FROM {stb} WHERE c0 >= 500"),
    ("Q5 WHERE t0", "SELECT * FROM {stb} WHERE t0 = 0"),
    ("Q6 GROUP t0", "SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
]


class TestPerfTagRefR4BigData:

    def setup_class(cls):
        apply_perf_flags()
        rpt.header("tag-ref R4: big data + many tables (children=200)")
        rpt.emit(f"  Rows sweep: {ROWS_SET}")
        rpt.emit(f"  children={CHILDREN}, data_cols={DATA_COLS}, tag_cols={TAG_COLS}, vgroups={VGROUPS}")
        rpt.emit(f"  Queries: Q2(COUNT), Q4(WHERE data), Q5(WHERE tag), Q6(GROUP BY tag)")

        cls._sweep = []
        for n_rows in ROWS_SET:
            db_name = f"pf_r4_r{n_rows}"
            env = build_tag_ref_env(
                db=db_name,
                children=CHILDREN,
                rows_per_child=n_rows,
                data_cols=DATA_COLS,
                tag_cols=TAG_COLS,
                vgroups=VGROUPS,
            )
            lit = f"{env['db']}.{env['vstb_literal']}"
            tref = f"{env['db']}.{env['vstb_tagref']}"

            results = {}
            for qname, qtpl in QUERIES:
                lit_ms = median(bench(qtpl.format(stb=lit)))
                tref_ms = median(bench(qtpl.format(stb=tref)))
                results[qname] = (lit_ms, tref_ms)

            cls._sweep.append({
                "rows": n_rows,
                "db": db_name,
                "results": results,
            })

    def test_01_scaling_matrix(self):
        """Tag-ref R4: overhead vs data volume

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Overhead vs Data Volume (children=200) ===")
        rpt.emit(f"  {'Rows':>10} | {'Q2 Lit':>10} {'Q2 TRef':>10} {'Q2 Over%':>10} | "
                 f"{'Q4 Lit':>10} {'Q4 TRef':>10} {'Q4 Over%':>10} | "
                 f"{'Q5 Lit':>10} {'Q5 TRef':>10} {'Q5 Over%':>10} | "
                 f"{'Q6 Lit':>10} {'Q6 TRef':>10} {'Q6 Over%':>10}")
        rpt.emit("  " + "-" * 160)

        for entry in self._sweep:
            n = entry["rows"]
            res = entry["results"]
            parts = []
            for qname in ["Q2 COUNT", "Q4 WHERE dt", "Q5 WHERE t0", "Q6 GROUP t0"]:
                lit_ms, tref_ms = res[qname]
                over = f"{(tref_ms - lit_ms) / lit_ms * 100:+.1f}%" if lit_ms > 0 else "-"
                parts.append(f"{lit_ms:>10.2f} {tref_ms:>10.2f} {over:>10}")
            rpt.emit(f"  {n:>10} | " + " | ".join(parts))

        rpt.flush()

    def test_02_overhead_percentage_trend(self):
        """Tag-ref R4: overhead percentage trend as data grows

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Overhead Percentage Trend (tag-ref vs literal) ===")
        rpt.emit(f"  {'Rows':>10} | {'Q5 Delta(ms)':>14} {'Q5 Over%':>10} | {'Q6 Delta(ms)':>14} {'Q6 Over%':>10}")
        rpt.emit("  " + "-" * 70)

        for entry in self._sweep:
            n = entry["rows"]
            res = entry["results"]
            parts = []
            for qname in ["Q5 WHERE t0", "Q6 GROUP t0"]:
                lit_ms, tref_ms = res[qname]
                delta = tref_ms - lit_ms
                over = f"{delta / lit_ms * 100:+.1f}%" if lit_ms > 0 else "-"
                parts.append((delta, over))
            rpt.emit(f"  {n:>10} | {parts[0][0]:>14.2f} {parts[0][1]:>10} | "
                     f"{parts[1][0]:>14.2f} {parts[1][1]:>10}")

        rpt.flush()

    def test_03_drowning_check(self):
        """Tag-ref R4: check if Q5 fixed overhead is constant regardless of data volume

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Q5 (WHERE tag) Fixed Overhead Check ===")
        rpt.emit(f"  {'Rows':>10} {'Total Rows':>14} {'Q5 Lit(ms)':>14} {'Q5 TRef(ms)':>14} "
                 f"{'Delta(ms)':>14} {'Over%':>10}")
        rpt.emit("  " + "-" * 80)

        for entry in self._sweep:
            n = entry["rows"]
            total = n * CHILDREN
            lit_ms, tref_ms = entry["results"]["Q5 WHERE t0"]
            delta = tref_ms - lit_ms
            over = f"{delta / lit_ms * 100:+.1f}%" if lit_ms > 0 else "-"
            rpt.emit(f"  {n:>10} {total:>14} {lit_ms:>14.2f} {tref_ms:>14.2f} {delta:>14.2f} {over:>10}")

        rpt.flush()

    def test_zz_summary(self):
        """Summary

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n" + "=" * 100)
        rpt.emit("tag-ref R4 big data: benchmark complete")
        rpt.emit("=" * 100)
        rpt.footer()
