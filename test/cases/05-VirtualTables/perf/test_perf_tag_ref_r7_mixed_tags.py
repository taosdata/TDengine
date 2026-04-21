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
"""Performance benchmark: tag-ref R7 mixed literal + tag-ref tags.

Measures performance of vstables with varying proportions of
tag-ref vs literal tags. total_tags=5, n_tag_refs=[0, 1, 3, 5].

Uses build_mixed_tag_env to create envs with mixed tag types.
Queries Q5 on different tag columns to compare literal vs tag-ref tag access.

Report written to /tmp/perf_tag_ref_r7_mixed_tags_report.txt
"""

from new_test_framework.utils import tdLog, tdSql
from perf_test_framework import apply_perf_flags, bench, median, PerfReport
from tag_ref_perf_util import build_mixed_tag_env

REPORT_FILE = "/tmp/perf_tag_ref_r7_mixed_tags_report.txt"
rpt = PerfReport(REPORT_FILE)

CHILDREN = 50
ROWS_PER_CHILD = 1000
DATA_COLS = 5
TOTAL_TAGS = 5
N_TAG_REFS_SET = [0, 1, 3, 5]
VGROUPS = 2

QUERIES = [
    ("Q5 WHERE t0", "SELECT * FROM {stb} WHERE t0 = 0"),
    ("Q5 WHERE t4", "SELECT * FROM {stb} WHERE t4 = 4"),
    ("Q6 GROUP t0", "SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
    ("Q6 GROUP t4", "SELECT t4, COUNT(*) FROM {stb} GROUP BY t4"),
]


class TestPerfTagRefR7MixedTags:

    def setup_class(cls):
        apply_perf_flags()
        rpt.header("tag-ref R7: mixed literal + tag-ref tags (total_tags=5)")
        rpt.emit(f"  Tag-ref proportions: {N_TAG_REFS_SET}")
        rpt.emit(f"  children={CHILDREN}, rows/child={ROWS_PER_CHILD}, "
                 f"data_cols={DATA_COLS}, total_tags={TOTAL_TAGS}, vgroups={VGROUPS}")
        rpt.emit(f"  First n_tag_refs tags are tag-ref, rest are literal")

        cls._sweep = []
        for n_refs in N_TAG_REFS_SET:
            db_name = f"pf_r7_m{n_refs}"
            env = build_mixed_tag_env(
                db=db_name,
                children=CHILDREN,
                rows_per_child=ROWS_PER_CHILD,
                data_cols=DATA_COLS,
                total_tags=TOTAL_TAGS,
                n_tag_refs=n_refs,
                vgroups=VGROUPS,
            )
            vstb = f"{env['db']}.{env['vstb_mixed']}"

            results = {}
            for qname, qtpl in QUERIES:
                ms = median(bench(qtpl.format(stb=vstb)))
                results[qname] = ms

            cls._sweep.append({
                "n_tag_refs": n_refs,
                "db": db_name,
                "vstb": vstb,
                "results": results,
            })

    def test_01_mixed_tag_matrix(self):
        """Tag-ref R7: performance with varying tag-ref proportion

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Mixed Tag Performance Matrix ===")
        rpt.emit(f"  Note: t0 is tag-ref when n_tag_refs>=1; t4 is tag-ref only when n_tag_refs=5")
        rpt.emit("")
        rpt.emit(f"  {'NTagRefs':>10} | {'Q5 t0(ms)':>12} {'Q5 t4(ms)':>12} | "
                 f"{'Q6 t0(ms)':>12} {'Q6 t4(ms)':>12} | {'t0 type':>10} {'t4 type':>10}")
        rpt.emit("  " + "-" * 100)

        for entry in self._sweep:
            n = entry["n_tag_refs"]
            res = entry["results"]
            t0_type = "tag-ref" if n >= 1 else "literal"
            t4_type = "tag-ref" if n >= 5 else "literal"
            rpt.emit(f"  {n:>10} | {res['Q5 WHERE t0']:>12.2f} {res['Q5 WHERE t4']:>12.2f} | "
                     f"{res['Q6 GROUP t0']:>12.2f} {res['Q6 GROUP t4']:>12.2f} | "
                     f"{t0_type:>10} {t4_type:>10}")

        rpt.flush()

    def test_02_tagref_proportion_overhead(self):
        """Tag-ref R7: Q5 overhead growth with tag-ref proportion

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        # Baseline is n_tag_refs=0 (all literal)
        baseline_t0 = self._sweep[0]["results"]["Q5 WHERE t0"]
        baseline_t4 = self._sweep[0]["results"]["Q5 WHERE t4"]

        rpt.emit("\n=== Q5 Overhead Growth with Tag-ref Proportion ===")
        rpt.emit(f"  Baseline (n_tag_refs=0): t0={baseline_t0:.2f}ms, t4={baseline_t4:.2f}ms")
        rpt.emit("")
        rpt.emit(f"  {'NTagRefs':>10} {'Q5 t0(ms)':>12} {'t0 vs base':>12} {'Q5 t4(ms)':>12} "
                 f"{'t4 vs base':>12} {'t0 type':>10} {'t4 type':>10}")
        rpt.emit("  " + "-" * 88)

        for entry in self._sweep:
            n = entry["n_tag_refs"]
            res = entry["results"]
            t0_type = "tag-ref" if n >= 1 else "literal"
            t4_type = "tag-ref" if n >= 5 else "literal"
            t0_vs = f"{(res['Q5 WHERE t0'] - baseline_t0) / baseline_t0 * 100:+.1f}%" if baseline_t0 > 0 else "-"
            t4_vs = f"{(res['Q5 WHERE t4'] - baseline_t4) / baseline_t4 * 100:+.1f}%" if baseline_t4 > 0 else "-"
            rpt.emit(f"  {n:>10} {res['Q5 WHERE t0']:>12.2f} {t0_vs:>12} "
                     f"{res['Q5 WHERE t4']:>12.2f} {t4_vs:>12} {t0_type:>10} {t4_type:>10}")

        rpt.flush()

    def test_03_literal_vs_tagref_column(self):
        """Tag-ref R7: compare querying a literal tag column vs a tag-ref column

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n=== Literal Tag Column (t4) vs Tag-ref Column (t0) ===")
        rpt.emit(f"  When n_tag_refs=3: t0=tag-ref, t4=literal")
        rpt.emit(f"  When n_tag_refs=5: t0=tag-ref, t4=tag-ref")
        rpt.emit("")
        rpt.emit(f"  {'NTagRefs':>10} {'Q5 t0(ms)':>12} {'Q5 t4(ms)':>12} {'Delta(ms)':>12} "
                 f"{'t0 type':>10} {'t4 type':>10}")
        rpt.emit("  " + "-" * 76)

        for entry in self._sweep:
            n = entry["n_tag_refs"]
            res = entry["results"]
            t0_type = "tag-ref" if n >= 1 else "literal"
            t4_type = "tag-ref" if n >= 5 else "literal"
            delta = res["Q5 WHERE t0"] - res["Q5 WHERE t4"]
            rpt.emit(f"  {n:>10} {res['Q5 WHERE t0']:>12.2f} {res['Q5 WHERE t4']:>12.2f} "
                     f"{delta:>+12.2f} {t0_type:>10} {t4_type:>10}")

        rpt.flush()

    def test_zz_summary(self):
        """Summary

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, tag-ref

        Jira: None

        History:
            - 2026-04-20 Created
        """
        rpt.emit("\n" + "=" * 100)
        rpt.emit("tag-ref R7 mixed tags: benchmark complete")
        rpt.emit("=" * 100)
        rpt.footer()
