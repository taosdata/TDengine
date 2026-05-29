###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form other than as expressly provided
#  by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""Performance benchmark: VTable-ref-VTable depth (VCT dimension) — single depth.

Usage:
  DEPTH=1  pytest -s cases/05-VirtualTables/perf/test_perf_vct_depth_one.py
  DEPTH=32 pytest -s cases/05-VirtualTables/perf/test_perf_vct_depth_one.py

Or use run_perf_vct_depth.sh to run all depths and merge results.
"""

import json
import os
import tempfile
from new_test_framework.utils import tdLog, tdSql, etool
from perf_test_framework import apply_perf_flags, bench, median

DB_NAME = "pf_vref_vct"
ROWS = 100000
DATA_COLS = 5
TAG_COLS = 3
VGROUPS = 1
REPEATS = 64

DEPTH = int(os.environ.get("DEPTH", "1"))

RESULT_FILE = f"/tmp/perf_vct_d{DEPTH}.json"

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
    """Use taosBenchmark to create 1 child table with ROWS rows."""
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


class TestPerfVCTDepth:

    def setup_class(cls):
        apply_perf_flags()
        tdLog.info(f"=== VCT depth={DEPTH} benchmark ===")

        # Step 1: Create source data
        tdLog.info("=== Step 1: taosBenchmark insert source data ===")
        _create_source_data()

        tdSql.execute(f"USE {DB_NAME};")
        col_defs = ", ".join(f"c{i} INT" for i in range(DATA_COLS))
        tag_defs = ", ".join(f"t{i} INT" for i in range(TAG_COLS))

        # Step 2: Build VCT chain up to DEPTH
        tdLog.info(f"=== Step 2: Build VCT chain (depth 1 → {DEPTH}) ===")
        vct_name = None
        for d in range(1, DEPTH + 1):
            vstb_name = f"vstb_l{d}"
            tdSql.execute(f"CREATE STABLE {vstb_name} (ts TIMESTAMP, {col_defs}) "
                          f"TAGS ({tag_defs}) VIRTUAL 1;")
            if d == 1:
                prev = "src_c0"
            else:
                prev = f"vct_d{d-1}"
            col_refs = ", ".join(f"c{i} FROM {prev}.c{i}" for i in range(DATA_COLS))
            vct_name = f"vct_d{d}"
            tdSql.execute(f"CREATE VTABLE {vct_name} ({col_refs}) "
                          f"USING {vstb_name} TAGS (0, 0, 0);")
            tdLog.info(f"    depth {d}/{DEPTH}: {vct_name} created")

        cls._vct = vct_name
        tdLog.info("=== Setup complete ===")

        # Step 3: Run benchmarks
        tdLog.info(f"=== Step 3: Running benchmarks ({REPEATS} samples) ===")
        cls._results = {}
        for qname, qtpl in QUERIES:
            sql = qtpl.format(tbl=cls._vct)
            cls._results[qname] = median(bench(sql, repeats=REPEATS))
            tdLog.info(f"    {qname}: {cls._results[qname]:.2f} ms")

        # Step 4: Write JSON result
        with open(RESULT_FILE, "w") as f:
            json.dump({"depth": DEPTH, "rows": ROWS, "repeats": REPEATS,
                        "results": cls._results}, f, indent=2)
        tdLog.info(f"Results written to {RESULT_FILE}")

    def test_vct_depth_one(self):
        """VCT depth benchmark — single depth point.

        Catalog: VirtualTable
        Since: v3.4.0.0
        Labels: virtual, performance, vtable-ref, depth, vct
        """
        tdLog.info(f"=== Depth={DEPTH} complete ===")
        for qname, _ in QUERIES:
            tdLog.info(f"  {qname}: {self._results[qname]:.2f} ms")
