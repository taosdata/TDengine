# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Opt-in benchmark for system-table and tag-scan constant projections."""

import argparse
import json
import re
import statistics
import time

import taos


def _quote_ident(name):
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError("--database must be an identifier")
    return f"`{name}`"


def prepare(cursor, database, tables, wide_tag_bytes):
    db_ident = _quote_ident(database)
    cursor.execute(f"DROP DATABASE IF EXISTS {db_ident}")
    cursor.execute(f"CREATE DATABASE {db_ident} VGROUPS 1")
    cursor.execute(f"CREATE STABLE {db_ident}.st (ts TIMESTAMP, v INT) TAGS (device INT)")
    for start in range(0, tables, 20):
        children = [
            f"{db_ident}.t{i} USING {db_ident}.st TAGS({i})"
            for i in range(start, min(start + 20, tables))
        ]
        cursor.execute("CREATE TABLE " + " ".join(children))
    if wide_tag_bytes:
        cursor.execute(
            f"CREATE STABLE {db_ident}.st_wide (ts TIMESTAMP, v INT) "
            f"TAGS (payload BINARY({wide_tag_bytes}))"
        )
        payload = "x" * wide_tag_bytes
        for start in range(0, tables, 20):
            children = [
                f"{db_ident}.w{i} USING {db_ident}.st_wide TAGS('{payload}')"
                for i in range(start, min(start + 20, tables))
            ]
            cursor.execute("CREATE TABLE " + " ".join(children))
    cursor.execute(f"FLUSH DATABASE {db_ident}")
    print(json.dumps({"database": database, "created_tables": tables, "wide_tag_bytes": wide_tag_bytes}))


def measure(cursor, database, tables, samples, warmups, wide_tag_bytes, query_name):
    db_ident = _quote_ident(database)
    count_sql = f"SELECT COUNT(*) FROM information_schema.ins_tables WHERE db_name = '{database}'"
    for attempt in range(5):
        cursor.execute(count_sql)
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("table count query returned no row; run --prepare first")
        actual_tables = row[0]
        # Extra tables cannot disappear by waiting; only a shortfall may be transient.
        expected_tables = tables * (2 if wide_tag_bytes else 1)
        if actual_tables >= expected_tables:
            break
        if attempt < 4:
            time.sleep(0.2)
    if actual_tables > expected_tables:
        raise RuntimeError(
            f"data set has {actual_tables} tables, expected {expected_tables}; "
            "recreate or clean the benchmark database"
        )
    if actual_tables < expected_tables:
        raise RuntimeError(
            f"data set has {actual_tables} tables, expected {expected_tables}; "
            "check --tables and run --prepare first"
        )
    queries = [
        ("system_constant", f"SELECT 1 FROM information_schema.ins_tables WHERE db_name = '{database}'", expected_tables),
        ("system_constant_all", "SELECT 1 FROM information_schema.ins_tables", None),
        ("tag_constant", f"SELECT TAGS 1 FROM {db_ident}.st", tables),
    ]
    if wide_tag_bytes:
        queries.append(("tag_constant_wide", f"SELECT TAGS 1 FROM {db_ident}.st_wide", tables))
    if query_name != "all":
        queries = [item for item in queries if item[0] == query_name]
        if not queries:
            raise ValueError(f"{query_name} requires --wide-tag-bytes")
    for name, sql, expected_rows in queries:
        if name == "system_constant_all":
            cursor.execute("SELECT table_name FROM information_schema.ins_tables")
            expected_rows = len(cursor.fetchall())
        for _ in range(warmups):
            cursor.execute(sql)
            cursor.fetchall()
        execute_ms = []
        fetch_ms = []
        for _ in range(samples):
            start = time.perf_counter_ns()
            cursor.execute(sql)
            after_execute = time.perf_counter_ns()
            rows = cursor.fetchall()
            after_fetch = time.perf_counter_ns()
            if not rows:
                raise RuntimeError(f"{name}: benchmark query returned no rows")
            if expected_rows is not None and len(rows) != expected_rows:
                raise RuntimeError(f"{name}: expected {expected_rows} rows, got {len(rows)}")
            if any(row != (1,) for row in rows):
                raise RuntimeError(f"{name}: constant projection returned an unexpected value")
            execute_ms.append(round((after_execute - start) / 1_000_000, 3))
            fetch_ms.append(round((after_fetch - after_execute) / 1_000_000, 3))
        total_ms = [round(a + b, 3) for a, b in zip(execute_ms, fetch_ms)]
        print(json.dumps({"query": name, "rows": len(rows), "execute_ms": execute_ms,
                          "fetch_ms": fetch_ms, "total_ms": total_ms,
                          "median_total_ms": round(statistics.median(total_ms), 3)}))


def verify_unfiltered_multiblock(cursor, database):
    cursor.execute("SELECT table_name FROM information_schema.ins_tables")
    expected_rows = len(cursor.fetchall())
    if expected_rows <= 32768:
        raise RuntimeError(f"expected more than 32768 system table rows, got {expected_rows}")
    cursor.execute("SELECT 1 FROM information_schema.ins_tables")
    rows = cursor.fetchall()
    if len(rows) != expected_rows or any(row != (1,) for row in rows):
        raise RuntimeError(f"unfiltered constant scan mismatch: expected {expected_rows} rows, got {len(rows)}")
    for offset in (32767, 32768):
        cursor.execute(f"SELECT 1 FROM information_schema.ins_tables LIMIT 3 OFFSET {offset}")
        page = cursor.fetchall()
        if page != [(1,)] * 3:
            raise RuntimeError(f"constant scan page mismatch at offset {offset}: {page}")
    print(json.dumps({"query": "system_constant_all_multiblock", "rows": expected_rows, "status": "passed"}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--prepare", action="store_true", help="create a fresh benchmark data set")
    action.add_argument("--measure", action="store_true", help="measure an existing data set")
    action.add_argument("--verify-unfiltered-multiblock", action="store_true",
                        help="check unfiltered system table scan across the 32768-row block boundary")
    parser.add_argument("--database", default="count_scan_bench")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--tables", type=int, default=10000)
    parser.add_argument("--samples", type=int, default=8)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--wide-tag-bytes", type=int, default=0)
    parser.add_argument("--query", choices=["all", "system_constant", "system_constant_all", "tag_constant", "tag_constant_wide"],
                        default="all")
    parser.add_argument("--config", required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    if args.tables < 1:
        parser.error("--tables must be positive")
    if args.measure and args.samples < 1:
        parser.error("--samples must be positive")
    if args.measure and args.warmups < 0:
        parser.error("--warmups must be nonnegative")
    if args.wide_tag_bytes < 0 or args.wide_tag_bytes > 1024:
        parser.error("--wide-tag-bytes must be between 0 and 1024")
    try:
        _quote_ident(args.database)
    except ValueError as exc:
        parser.error(str(exc))

    connection = taos.connect(host=args.host, port=args.port, config=args.config)
    cursor = connection.cursor()
    try:
        if args.prepare:
            prepare(cursor, args.database, args.tables, args.wide_tag_bytes)
        elif args.measure:
            measure(cursor, args.database, args.tables, args.samples, args.warmups, args.wide_tag_bytes, args.query)
        else:
            verify_unfiltered_multiblock(cursor, args.database)
    finally:
        try:
            cursor.close()
        finally:
            connection.close()


if __name__ == "__main__":
    main()
