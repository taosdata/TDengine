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
"""Shared framework for virtual-table performance benchmarks."""

import statistics
import time
from new_test_framework.utils import tdLog, tdSql

BASE_TS = 1700000000000
REPEATS = 5


def apply_perf_flags():
    tdSql.execute('ALTER ALL DNODES "debugFlag 131";')
    tdSql.execute('ALTER LOCAL "debugFlag 131";')


def bench(sql, repeats=REPEATS):
    """Run SQL N times, return list of elapsed ms."""
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        tdSql.query(sql)
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000.0)
    return times


def median(values):
    return statistics.median(values)


def insert_rows(table, n_rows, n_cols=5, batch=1000):
    """Insert n_rows rows with n_cols data columns into table."""
    for start in range(0, n_rows, batch):
        end = min(start + batch, n_rows)
        vals = []
        for i in range(start, end):
            cols = ", ".join(str(i + j * 100) for j in range(n_cols))
            vals.append(f"({BASE_TS + i}, {cols})")
        tdSql.execute(f"INSERT INTO {table} VALUES " + ", ".join(vals))


class PerfReport:
    """Accumulates report lines and flushes to file."""

    def __init__(self, filename):
        self._filename = filename
        self._lines = []

    def emit(self, line=""):
        self._lines.append(line)
        tdLog.info(line)

    def flush(self):
        with open(self._filename, "w") as fp:
            fp.write("\n".join(self._lines) + "\n")
        tdLog.info(f"Report written to {self._filename}")

    def header(self, title):
        self._lines.clear()
        self.emit(f"PERF: {title}")
        self.emit("=" * 100)

    def footer(self):
        self.emit("\n" + "=" * 100)
        self.emit("benchmark complete")
        self.emit("=" * 100)
        self.flush()
