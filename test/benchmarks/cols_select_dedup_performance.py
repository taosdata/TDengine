# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Standalone, identical-workload benchmark for cols selector reuse."""

import argparse
import hashlib
import json
import statistics
import time
from datetime import datetime, timezone

import taos


DB = "cols_dedup_perf"
TABLE = f"{DB}.samples"
BASE_TS = 1750000000000


def query_for(count):
    projections = ", ".join(
        "cols(max(c0), c1 as v)" for _ in range(count)
    )
    return f"select {projections} from {TABLE}"


def fetch(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()


def prepare(cursor, rows, batch_size):
    cursor.execute(f"drop database if exists {DB}")
    cursor.execute(f"create database {DB} vgroups 1 keep 36500")
    cursor.execute(
        f"create table {TABLE} (ts timestamp, c0 int, c1 int, c2 int, "
        "c3 int, c4 int, c5 int)"
    )
    for start in range(0, rows, batch_size):
        values = []
        for index in range(start, min(start + batch_size, rows)):
            columns = ",".join(str(index * factor) for factor in range(1, 7))
            values.append(f"({BASE_TS + index},{columns})")
        cursor.execute(f"insert into {TABLE} values " + " ".join(values))
        if (start // batch_size + 1) % 100 == 0:
            print(f"inserted {min(start + batch_size, rows):,}/{rows:,} rows", flush=True)
    cursor.execute(f"flush database {DB}")


def measure(cursor, rounds, warmups, expected_rows):
    count = fetch(cursor, f"select count(*) from {TABLE}")[0][0]
    if count != expected_rows:
        raise RuntimeError(f"expected {expected_rows} rows, found {count}")
    cases = {}
    for width in range(1, 6):
        sql = query_for(width)
        rows = fetch(cursor, sql)
        expected = ((count - 1) * 2,) * width
        if len(rows) != 1 or tuple(rows[0]) != expected:
            raise RuntimeError(f"unexpected result for case {width}: {rows!r}")
        plan = fetch(cursor, "explain verbose true " + sql)
        cases[str(width)] = {
            "sql": sql,
            "result_sha256": hashlib.sha256(repr(rows).encode()).hexdigest(),
            "plan": [str(row[0]) for row in plan],
            "samples_ms": [],
        }
    for _ in range(warmups):
        for width in range(1, 6):
            fetch(cursor, cases[str(width)]["sql"])
    for round_index in range(rounds):
        order = range(1, 6) if round_index % 2 == 0 else range(5, 0, -1)
        for width in order:
            start = time.perf_counter_ns()
            rows = fetch(cursor, cases[str(width)]["sql"])
            elapsed = (time.perf_counter_ns() - start) / 1_000_000
            case = cases[str(width)]
            if hashlib.sha256(repr(rows).encode()).hexdigest() != case["result_sha256"]:
                raise RuntimeError(f"result changed for case {width}")
            case["samples_ms"].append(round(elapsed, 3))
        print(f"measured round {round_index + 1}/{rounds}", flush=True)
    for case in cases.values():
        case["median_ms"] = round(statistics.median(case["samples_ms"]), 3)
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "measure"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6030)
    parser.add_argument("--config", help="taos.cfg directory for the client")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--rounds", type=int, default=15)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--output", help="JSON result file for measure mode")
    args = parser.parse_args()
    if args.rows < 1 or args.batch_size < 1 or args.rounds < 1 or args.warmups < 0:
        parser.error("rows, batch-size and rounds must be positive; warmups must be nonnegative")
    if args.mode == "measure" and not args.output:
        parser.error("measure requires --output")

    connection = taos.connect(
        host=args.host, port=args.port, user="root", password="taosdata",
        config=args.config,
    )
    try:
        cursor = connection.cursor()
        if args.mode == "prepare":
            prepare(cursor, args.rows, args.batch_size)
        else:
            result = {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "host": args.host,
                "port": args.port,
                "rows": args.rows,
                "rounds": args.rounds,
                "warmups": args.warmups,
                "cases": measure(cursor, args.rounds, args.warmups, args.rows),
            }
            with open(args.output, "w", encoding="utf-8") as output:
                json.dump(result, output, indent=2, ensure_ascii=False)
            for width, case in result["cases"].items():
                print(f"{width} cols: median {case['median_ms']:.3f} ms")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
