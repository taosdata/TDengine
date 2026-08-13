#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Stream notify latency benchmark via taosd [notify_trace] logs.

Runs 20 streams × 30 rows (count_window(1): one notify per row), then parses
streamSendNotifyContent() trace lines from taosd log:

    [notify_trace] streamId:<id> gid:<gid> total_us:<us> ...

Reports distribution of total_us (µs): min, mean, p50, p90, p99, max.
"""

import glob
import os
import re
import shutil
import time
from typing import Dict, List, Optional

from new_test_framework.utils import tdDnodes, tdLog, tdSql, tdStream
from stream_notify_server import start_notify_server_background, stop_notify_server_background

# ── configuration ──────────────────────────────────────────────────────────────
NOTIFY_PORT = 18765
STREAM_COUNT = 20
ROWS_PER_STREAM = 30
WARMUP_STREAMS = 2
WARMUP_ROWS = 5
ROW_TIMEOUT_S = 180
BASE_TS_MS = 1_700_000_000_000
TS_STEP_MS = 1
TS_BASE_GAP_MS = 100_000  # per-stream timestamp offset

CALLER_DIR = os.path.dirname(os.path.realpath(__file__))
NOTIFY_LOG_DIR = os.path.join(CALLER_DIR, "notify_result_tmp_latency")

# streamSendNotifyContent trace: first total_us is the function wall time (µs)
NOTIFY_TRACE_RE = re.compile(
    r"\[notify_trace\]\s+streamId:[0-9a-f]+\s+gid:\d+\s+total_us:(\d+)",
    re.IGNORECASE,
)
# ───────────────────────────────────────────────────────────────────────────────


class TestStreamWriteLatency:
    db_name = "test_stream_write_latency"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.query("show snodes")
        if tdSql.getRows() == 0:
            tdStream.createSnode()

    # ── helpers ────────────────────────────────────────────────────────────────

    @staticmethod
    def _drop_database_compat(db_name: str):
        try:
            tdSql.execute(f"drop database if exists {db_name}")
        except Exception as e:
            if "virtual table stream exists" in str(e).lower():
                tdSql.execute(f"drop database if exists {db_name} force")
            else:
                raise

    @staticmethod
    def _prepare_log_dir():
        if os.path.isdir(NOTIFY_LOG_DIR):
            shutil.rmtree(NOTIFY_LOG_DIR, ignore_errors=True)
        os.makedirs(NOTIFY_LOG_DIR, exist_ok=True)

    @staticmethod
    def _taosd_log_files() -> List[str]:
        log_dir = os.path.join(tdDnodes.getDnodeDir(1), "log")
        files = sorted(glob.glob(os.path.join(log_dir, "taosdlog*")))
        return [f for f in files if os.path.isfile(f)]

    @staticmethod
    def _log_offsets() -> Dict[str, int]:
        return {path: os.path.getsize(path) for path in TestStreamWriteLatency._taosd_log_files()}

    @staticmethod
    def _parse_notify_trace_since(offsets: Dict[str, int]) -> List[int]:
        """Read taosd logs from recorded offsets and extract total_us samples."""
        samples: List[int] = []
        for path in TestStreamWriteLatency._taosd_log_files():
            start = offsets.get(path, 0)
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    f.seek(start)
                    for line in f:
                        m = NOTIFY_TRACE_RE.search(line)
                        if m:
                            samples.append(int(m.group(1)))
            except FileNotFoundError:
                continue
        return samples

    @staticmethod
    def _stats_us(vals: List[int]) -> Dict:
        if not vals:
            return {
                "count": 0,
                "min": None,
                "max": None,
                "mean": None,
                "p50": None,
                "p90": None,
                "p99": None,
            }
        s = sorted(vals)
        n = len(s)

        def pct(p: int) -> float:
            idx = min(int(n * p / 100), n - 1)
            return round(s[idx], 2)

        return {
            "count": n,
            "min": s[0],
            "max": s[-1],
            "mean": round(sum(s) / n, 2),
            "p50": pct(50),
            "p90": pct(90),
            "p99": pct(99),
        }

    @staticmethod
    def _wait_table_rows(table_name: str, expected_rows: int, timeout_s: float) -> int:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                tdSql.query(f"select count(*) from {table_name}")
                rows = tdSql.getData(0, 0)
                if rows >= expected_rows:
                    return rows
            except Exception as e:
                if "does not exist" not in str(e).lower():
                    raise
            time.sleep(0.2)
        tdLog.exit(
            f"table {table_name} did not reach {expected_rows} rows within {timeout_s}s"
        )

    def _create_streams(self, stream_count: int, notify_url_tpl: str):
        for i in range(stream_count):
            src = f"src_{i}"
            out = f"out_{i}"
            stream = f"s_{i}"
            tdSql.execute(f"create table {src}(ts timestamp, v int)")
            tdSql.execute(
                f"create stream {stream} count_window(1) from {src} "
                f"notify('{notify_url_tpl.format(i=i)}') on(window_close) "
                f"into {out} as select _twstart as wstart, count(*) as cnt from %%trows"
            )
            tdStream.checkStreamStatus(stream)

    def _insert_rows(self, stream_count: int, rows_per_stream: int, base_ts_ms: int):
        for i in range(stream_count):
            src = f"src_{i}"
            ts_base = base_ts_ms + i * TS_BASE_GAP_MS
            for j in range(rows_per_stream):
                ts_ms = ts_base + j * TS_STEP_MS
                tdSql.execute(f"insert into {src} values ({ts_ms}, {j})")

    def _wait_all_outputs(self, stream_count: int, rows_per_stream: int):
        for i in range(stream_count):
            self._wait_table_rows(f"out_{i}", rows_per_stream, ROW_TIMEOUT_S)

    @staticmethod
    def _print_notify_trace_stats(stats: Dict, expected: int):
        sep = "=" * 72
        print(sep, flush=True)
        print("  NOTIFY_TRACE BENCHMARK (streamSendNotifyContent total_us)", flush=True)
        print(sep, flush=True)
        print(f"  Streams      : {STREAM_COUNT}", flush=True)
        print(f"  Rows/stream  : {ROWS_PER_STREAM}", flush=True)
        print(f"  Expected     : {expected} trace samples", flush=True)
        print(f"  Collected    : {stats['count']} trace samples", flush=True)
        if stats["count"] == 0:
            print("  total_us     : no [notify_trace] lines found in taosd log", flush=True)
            print(sep, flush=True)
            return
        print(
            f"  total_us (µs): count={stats['count']}  "
            f"min={stats['min']}  mean={stats['mean']}  "
            f"p50={stats['p50']}  p90={stats['p90']}  p99={stats['p99']}  "
            f"max={stats['max']}",
            flush=True,
        )
        print(sep, flush=True)

    # ── test entry point ───────────────────────────────────────────────────────

    def test_stream_write_latency(self):
        """Benchmark streamSendNotifyContent via [notify_trace] taosd logs.

        1. Enable stdebugflag 143 so stTrace is emitted
        2. Create 20 streams, insert 30 rows each
        3. Parse total_us from taosd log and print distribution stats

        Catalog:
            - Stream Processing

        Since: v3.3.7.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-03 Copilot Created
            - 2026-06-08 notify_trace log parsing benchmark
        """
        expected_samples = STREAM_COUNT * ROWS_PER_STREAM
        self._prepare_log_dir()
        start_notify_server_background(
            port=NOTIFY_PORT,
            log_path=NOTIFY_LOG_DIR
        )
        try:
            tdSql.execute("alter all dnodes 'stdebugflag 143'")

            self._drop_database_compat(self.db_name)
            tdSql.execute(f"create database {self.db_name} vgroups 1")
            tdSql.execute(f"use {self.db_name}")

            notify_url_tpl = f"ws://localhost:{NOTIFY_PORT}/stream_{{i}}"

            # Warm-up: prime stream runtime and notify worker connections.
            msg = (
                f"[notify_trace] warm-up {WARMUP_STREAMS} streams × "
                f"{WARMUP_ROWS} rows ..."
            )
            tdLog.info(msg)
            print(msg, flush=True)
            self._create_streams(WARMUP_STREAMS, notify_url_tpl)
            self._insert_rows(WARMUP_STREAMS, WARMUP_ROWS, BASE_TS_MS)
            self._wait_all_outputs(WARMUP_STREAMS, WARMUP_ROWS)
            for i in range(WARMUP_STREAMS):
                tdSql.execute(f"drop stream if exists s_{i}")
                tdSql.execute(f"drop table if exists out_{i}")
                tdSql.execute(f"drop table if exists src_{i}")

            # Benchmark: record log offset so warm-up traces are excluded.
            log_offsets = self._log_offsets()
            msg = (
                f"[notify_trace] benchmark {STREAM_COUNT} streams × "
                f"{ROWS_PER_STREAM} rows ..."
            )
            tdLog.info(msg)
            print(msg, flush=True)

            self._create_streams(STREAM_COUNT, notify_url_tpl)
            self._insert_rows(
                STREAM_COUNT,
                ROWS_PER_STREAM,
                BASE_TS_MS + 1_000_000,
            )
            self._wait_all_outputs(STREAM_COUNT, ROWS_PER_STREAM)

            # Allow trace lines to flush to disk.
            time.sleep(1.0)
            samples = self._parse_notify_trace_since(log_offsets)
            stats = self._stats_us(samples)
            self._print_notify_trace_stats(stats, expected_samples)

            if stats["count"] == 0:
                tdLog.exit(
                    "[notify_trace] no trace samples found; "
                    "ensure stdebugflag 143 is enabled and taosd log is readable"
                )
            if stats["count"] < expected_samples * 0.8:
                tdLog.info(
                    f"[notify_trace] warning: only {stats['count']}/{expected_samples} "
                    f"trace samples collected"
                )
        finally:
            stop_notify_server_background()
            self._drop_database_compat(self.db_name)
