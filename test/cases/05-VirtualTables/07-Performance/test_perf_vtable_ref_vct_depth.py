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
"""Performance benchmark: VTable-ref-VTable depth (VCT dimension).

Topology:
  src_c0 (physical child table, 10K rows)
    ← vct_d1 (virtual child table, col-ref src_c0)
    ← vct_d2 (col-ref vct_d1)
    ← ... ← vct_d32

Single chain, single table queries. No fan-out overhead.
Tests pure recursive column resolution depth.

Depths: 1 (baseline), 2, 4, 8, 16, 32
Queries: A1-A10
Data insertion: taosBenchmark (1 child × 10K rows)

Report: /tmp/perf_vtable_ref_vct_depth_report.txt
"""

import json
import os
import tempfile
from new_test_framework.utils import tdLog, tdSql, etool
from perf_test_framework import apply_perf_flags, bench, median, PerfReport

REPORT_FILE = "/tmp/perf_vtable_ref_vct_depth_report.txt"
rpt = PerfReport(REPORT_FILE)

DB_NAME = "pf_vref_vct"
ROWS = 100000
DATA_COLS = 5
TAG_COLS = 3
VGROUPS = 1

DEPTH_POINTS = [1, 2, 4, 8, 16, 32]
MAX_DEPTH = 32

QUERIES = [
    ("A1  COUNT",       "SELECT COUNT(*) FROM {tbl}"),
    ("A2  SUM/AVG/MAX", "SELECT SUM(c0), AVG(c1), MAX(c2) FROM {tbl}"),
    ("A3  LAST",        "SELECT LAST(c0), LAST(c1) FROM {tbl}"),
    ("A4  WHERE >",     "SELECT * FROM {tbl} WHERE c0 > 50"),
    ("A5  SELECT *",    "SELECT * FROM {tbl}"),
    ("A6  BETWEEN",     "SELECT c0, c1 FROM {tbl} WHERE c0 BETWEEN 20 AND 80"),
    ("A7  AVG/STDDEV",  "SELECT AVG(c0), STDDEV(c1) FROM {tbl}"),
    ("A8  FIRST/LAST",  "SELECT FIRST(c0), LAST(c0) FROM {tbl}"),
    ("A9  ORDER BY",    "SELECT * FROM {tbl} ORDER BY c0"),
    ("A10 LIMIT",       "SELECT c0, c1, c2, c3, c4 FROM {tbl} LIMIT 5"),
]


def _create_source_data():
    """Use taosBenchmark to create 1 child table with 10K rows."""
    columns = [{"type": "INT", "name": f"c{i}"} for i in range(DATA_COLS)]
    tags = [{"type": "INT", "name": f"t{i}", "max": 100, "min": 0}
            for i in range(TAG_COLS)]

    config = {
        "filetype": "insert",
        "cfgdir": "/etc/taos",
        "host": "127.0.0.1",
        "port": 6030,
        "num_of_records_per_req": 5000,
        "thread_count": 1,
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
                "childtable_count": 1,
                "childtable_prefix": "src_c",
                "insert_rows": ROWS,
                "timestamp_step": 1000,
                "start_timestamp": "2024-01-01 00:00:00.000",
                "columns": columns,
                "tags": tags
            }]
        }]
    }

    fd, path = tempfile.mkstemp(suffix=".json", prefix="bench_vref_vct_")
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(config, f)
        etool.benchMark(json=path)
    finally:
        os.unlink(path)


class TestPerfVtableRefVCT:

    def setup_class(cls):
        apply_perf_flags()
        rpt.header("VTable-ref-VTable Depth (VCT dimension - single chain)")
        rpt.emit(f"  rows={ROWS:,}, data_cols={DATA_COLS}, vgroups={VGROUPS}")
        rpt.emit(f"  depths={DEPTH_POINTS}")
        rpt.emit("")

        # Step 1: Create source data
        tdLog.info("=== Step 1: taosBenchmark insert source data ===")
        _create_source_data()

        tdSql.execute(f"USE {DB_NAME};")
        col_defs = ", ".join(f"c{i} INT" for i in range(DATA_COLS))
        tag_defs = ", ".join(f"t{i} INT" for i in range(TAG_COLS))

        # Step 2: Build single VCT chain: src_c0 ← vct_d1 ← vct_d2 ← ... ← vct_d32
        tdLog.info("=== Step 2: Build VCT chain ===")
        # Need a vstable for each layer to create vtables under
        cls._vct = {}
        for depth in range(1, MAX_DEPTH + 1):
            vstb_name = f"vstb_l{depth}"
            tdSql.execute(f"CREATE STABLE {vstb_name} (ts TIMESTAMP, {col_defs}) "
                          f"TAGS ({tag_defs}) VIRTUAL 1;")
            if depth == 1:
                prev = "src_c0"
            else:
                prev = f"vct_d{depth-1}"
            col_refs = ", ".join(f"c{i} FROM {prev}.c{i}" for i in range(DATA_COLS))
            vct_name = f"vct_d{depth}"
            tdSql.execute(f"CREATE VTABLE {vct_name} ({col_refs}) "
                          f"USING {vstb_name} TAGS (0, 0, 0);")
            if depth in DEPTH_POINTS:
                cls._vct[depth] = vct_name
                tdLog.info(f"    vct_d{depth} created")

        tdLog.info("=== Setup complete ===")

        # Step 3: Run benchmarks (RESET QUERY CACHE before each depth)
        tdLog.info("=== Step 3: Running benchmarks ===")
        cls._results = {}
        for qname, qtpl in QUERIES:
            cls._results[qname] = {}
            for dp in DEPTH_POINTS:
                tdSql.execute("RESET QUERY CACHE")
                sql = qtpl.format(tbl=cls._vct[dp])
                cls._results[qname][dp] = median(bench(sql))
                tdLog.info(f"    {qname} D{dp}: {cls._results[qname][dp]:.2f} ms")

    def test_vtable_ref_vct_depth(self):
        """VTable-ref-VTable depth performance (VCT dimension - single chain)

        Perf benchmark measurement.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, performance, vtable-ref, depth, vct

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
        rpt.emit(f"  Topology: src_c0 ← vct_d1 ← vct_d2 ← ... ← vct_d32 (single chain)")
        rpt.emit(f"  Data: 1 child × {ROWS:,} rows")
        rpt.emit(f"  No fan-out — pure recursive depth measurement")
        rpt.footer()
