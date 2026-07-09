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
"""Performance benchmark: VTable-ref-VTable depth (VST dimension).

Topology:
  src_stb (physical, 10K children × 10K rows) ← vstb_d1 ← vstb_d2 ← ... ← vstb_d32
  Each vstb_d{N} has 10K VCT with col-ref to previous layer.
  Tags are literal (no tag-ref interference).

Depths: 1 (baseline), 2, 4, 8, 16, 32
Queries: A1-A10
Data insertion: taosBenchmark

Report: /tmp/perf_vtable_ref_vst_depth_report.txt
"""

import json
import os
import tempfile
from new_test_framework.utils import tdLog, tdSql, etool
from perf_test_framework import apply_perf_flags, bench, median, PerfReport

REPORT_FILE = "/tmp/perf_vtable_ref_vst_depth_report.txt"
rpt = PerfReport(REPORT_FILE)

DB_NAME = "pf_vref_vst"
CHILDREN = 10000
ROWS_PER_CHILD = 10000
DATA_COLS = 5
TAG_COLS = 3
VGROUPS = 4

DEPTH_POINTS = [1, 2, 4, 8, 16, 32]
MAX_DEPTH = 32

QUERIES = [
    ("A1  COUNT",       "SELECT COUNT(*) FROM {stb}"),
    ("A2  SUM/AVG/MAX", "SELECT SUM(c0), AVG(c1), MAX(c2) FROM {stb}"),
    ("A3  LAST",        "SELECT LAST(c0), LAST(c1) FROM {stb}"),
    ("A4  WHERE data",  "SELECT * FROM {stb} WHERE c0 > 50"),
    ("A5  WHERE tag=",  "SELECT * FROM {stb} WHERE t0 = 500"),
    ("A6  WHERE range", "SELECT * FROM {stb} WHERE t0 BETWEEN 100 AND 200"),
    ("A7  GROUP BY",    "SELECT t0, COUNT(*) FROM {stb} GROUP BY t0"),
    ("A8  DISTINCT",    "SELECT DISTINCT t0, t1 FROM {stb}"),
    ("A9  tag+data",    "SELECT * FROM {stb} WHERE t0 = 500 AND c0 > 50"),
    ("A10 PARTITION",   "SELECT t0, SUM(c0) FROM {stb} PARTITION BY t0"),
]


def _tag_vals(c, tag_cols):
    return ", ".join(str(c * 10 + i) for i in range(tag_cols))


def _create_source_data():
    """Use taosBenchmark to create src_stb with 10K children × 10K rows."""
    columns = [{"type": "INT", "name": f"c{i}"} for i in range(DATA_COLS)]
    tags = [{"type": "INT", "name": f"t{i}", "max": CHILDREN * 10 + TAG_COLS, "min": 0}
            for i in range(TAG_COLS)]

    config = {
        "filetype": "insert",
        "cfgdir": "/etc/taos",
        "host": "127.0.0.1",
        "port": 6030,
        "num_of_records_per_req": 5000,
        "thread_count": 8,
        "confirm_parameter_prompt": "no",
        "databases": [{
            "dbinfo": {
                "name": DB_NAME,
                "drop": "yes",
                "vgroups": VGROUPS
            },
            "super_tables": [{
                "name": "src_stb",
                "child_table_exists": "no",
                "childtable_count": CHILDREN,
                "childtable_prefix": "src_c",
                "insert_rows": ROWS_PER_CHILD,
                "timestamp_step": 1000,
                "start_timestamp": "2024-01-01 00:00:00.000",
                "columns": columns,
                "tags": tags
            }]
        }]
    }

    fd, path = tempfile.mkstemp(suffix=".json", prefix="bench_vref_vst_")
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(config, f)
        etool.benchMark(json=path)
    finally:
        os.unlink(path)


class TestPerfVtableRefVST:

    def setup_class(cls):
        apply_perf_flags()
        rpt.header("VTable-ref-VTable Depth (VST dimension)")
        rpt.emit(f"  children={CHILDREN}, rows/child={ROWS_PER_CHILD:,}, "
                 f"total={CHILDREN * ROWS_PER_CHILD:,} rows")
        rpt.emit(f"  data_cols={DATA_COLS}, tag_cols={TAG_COLS}, vgroups={VGROUPS}")
        rpt.emit(f"  depths={DEPTH_POINTS}")
        rpt.emit("")

        # Step 1: Create source data via taosBenchmark
        tdLog.info("=== Step 1: taosBenchmark insert source data ===")
        _create_source_data()

        tdSql.execute(f"USE {DB_NAME};")
        col_defs = ", ".join(f"c{i} INT" for i in range(DATA_COLS))
        tag_defs = ", ".join(f"t{i} INT" for i in range(TAG_COLS))

        # Step 2: Build vstable chain depth 1 → 32
        # vstb_d1: col-ref → src_stb children
        # vstb_d2: col-ref → vstb_d1 children
        # ...
        tdLog.info("=== Step 2: Build vstable chain ===")
        cls._vstb = {}
        for depth in range(1, MAX_DEPTH + 1):
            vstb_name = f"vstb_d{depth}"
            tdSql.execute(f"CREATE STABLE {vstb_name} (ts TIMESTAMP, {col_defs}) "
                          f"TAGS ({tag_defs}) VIRTUAL 1;")
            for c in range(CHILDREN):
                if depth == 1:
                    prev_child = f"src_c{c}"
                else:
                    prev_child = f"vstb_d{depth-1}_c{c}"
                col_refs = ", ".join(f"c{i} FROM {prev_child}.c{i}" for i in range(DATA_COLS))
                tag_vals = _tag_vals(c, TAG_COLS)
                tdSql.execute(f"CREATE VTABLE {vstb_name}_c{c} ({col_refs}) "
                              f"USING {vstb_name} TAGS ({tag_vals});")
            if depth in DEPTH_POINTS:
                cls._vstb[depth] = vstb_name
                tdLog.info(f"    vstb_d{depth} created ({CHILDREN} VCT)")

        tdLog.info("=== Setup complete ===")

        # Step 3: Run benchmarks
        tdLog.info("=== Step 3: Running benchmarks ===")
        cls._results = {}
        for qname, qtpl in QUERIES:
            cls._results[qname] = {}
            for dp in DEPTH_POINTS:
                sql = qtpl.format(stb=cls._vstb[dp])
                cls._results[qname][dp] = median(bench(sql))
                tdLog.info(f"    {qname} D{dp}: {cls._results[qname][dp]:.2f} ms")

    def test_vtable_ref_vst_depth(self):
        """VTable-ref-VTable depth performance (VST dimension)

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vtable-ref, depth

        Jira: None

        History:
            - 2026-05-22 Created
        """
        # Absolute latency
        rpt.emit("\n┌─ Absolute Latency (ms) ─────────────────────────────────────────┐")
        header = f"  {'Query':<16}" + "".join(f"{'D'+str(d):>10}" for d in DEPTH_POINTS)
        rpt.emit(header)
        rpt.emit("  " + "─" * (16 + 10 * len(DEPTH_POINTS)))

        for qname, _ in QUERIES:
            row = f"  {qname:<16}"
            for dp in DEPTH_POINTS:
                row += f"{self._results[qname][dp]:>10.2f}"
            rpt.emit(row)

        # Relative to D1
        rpt.emit("")
        rpt.emit("┌─ Overhead vs D1 Baseline (%) ──────────────────────────────────────┐")
        header = f"  {'Query':<16}" + "".join(f"{'D'+str(d):>10}" for d in DEPTH_POINTS)
        rpt.emit(header)
        rpt.emit("  " + "─" * (16 + 10 * len(DEPTH_POINTS)))

        for qname, _ in QUERIES:
            row = f"  {qname:<16}"
            base = self._results[qname][1]
            for dp in DEPTH_POINTS:
                val = self._results[qname][dp]
                if base > 0:
                    pct = (val - base) / base * 100
                    row += f"{pct:>+9.1f}%"
                else:
                    row += f"{'N/A':>10}"
            rpt.emit(row)

        # D32 vs D1
        rpt.emit("")
        rpt.emit("┌─ D32 vs D1 Summary ────────────────────────────────────────────────┐")
        rpt.emit(f"  {'Query':<16} {'D1(ms)':>10} {'D32(ms)':>10} {'Overhead':>10} {'Delta(ms)':>12}")
        rpt.emit("  " + "─" * 60)

        for qname, _ in QUERIES:
            d1 = self._results[qname][1]
            d32 = self._results[qname][32]
            overhead = f"{(d32 - d1) / d1 * 100:+.1f}%" if d1 > 0 else "N/A"
            delta = f"{d32 - d1:+.2f}"
            rpt.emit(f"  {qname:<16} {d1:>10.2f} {d32:>10.2f} {overhead:>10} {delta:>12}")

        rpt.emit("")
        rpt.emit(f"  Topology: src_stb ← vstb_d1 ← vstb_d2 ← ... ← vstb_d32")
        rpt.emit(f"  Tags: literal (no tag-ref)")
        rpt.emit(f"  Data: {CHILDREN:,} children × {ROWS_PER_CHILD:,} rows = "
                 f"{CHILDREN * ROWS_PER_CHILD:,} total")
        rpt.footer()
