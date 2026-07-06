import json
import subprocess
import tempfile
import time
import os
import datetime
import taos

from new_test_framework.utils import tdLog, tdSql, tdStream


# ---------------------------------------------------------------------------
# Configuration. Scale can be reduced via env for fast validation, e.g.
#   VTBPERF_ROWS=2000 VTBPERF_CHILDREN=10 pytest ...
# ---------------------------------------------------------------------------
_VGROUPS      = 8
_CHILD_COUNT  = int(os.environ.get("VTBPERF_CHILDREN", "100"))
_INSERT_ROWS  = int(os.environ.get("VTBPERF_ROWS", "100000"))
_TIMESTAMP_STEP_MS = 5
_DATA_START   = "2020-10-01 00:00:00.000"
_DATA_START_MS = int(datetime.datetime(2020, 10, 1).timestamp() * 1000)


def _bench_json(cfgdir, dbname, stb, col_type, col_name, prefix,
                rows, drop, child_exists):
    return {
        "filetype": "insert",
        "cfgdir": cfgdir,
        "host": "127.0.0.1",
        "port": 6030,
        "user": "root",
        "password": "taosdata",
        "thread_count": 10,
        "num_of_records_per_req": 10000,
        "confirm_parameter_prompt": "no",
        "databases": [{
            "dbinfo": {"name": dbname, "drop": drop,
                       "vgroups": _VGROUPS, "precision": "ms"},
            "super_tables": [{
                "name": stb,
                "child_table_exists": child_exists,
                "childtable_count": _CHILD_COUNT,
                "childtable_prefix": prefix,
                "insert_rows": rows,
                "timestamp_step": _TIMESTAMP_STEP_MS,
                "start_timestamp": _DATA_START,
                "columns": [{"type": col_type, "name": col_name}],
                "tags": [{"type": "TINYINT", "name": "groupid", "max": 10, "min": 1}],
            }],
        }],
    }


def _run_benchmark(json_data):
    fd, path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    try:
        with open(path, "w") as f:
            json.dump(json_data, f)
        proc = subprocess.Popen(f"taosBenchmark -f {path}", shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _out, err = proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"taosBenchmark failed (rc={proc.returncode}):\n{err.decode()}")
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


def _wait_rows(conn, db, stb, expected, timeout=300):
    deadline = time.time() + timeout
    cur = conn.cursor()
    while time.time() < deadline:
        cur.execute(f"select count(*) from {db}.{stb}")
        r = cur.fetchone()
        if r and r[0] >= expected:
            return
        time.sleep(2)
    raise TimeoutError(f"{db}.{stb}: expected >={expected} rows, timed out")


def _wait_stream_result(conn, db, res_tb, min_rows, timeout=600):
    cur = conn.cursor()
    start = time.time()
    while time.time() - start < timeout:
        try:
            cur.execute(f"select count(*) from {db}.{res_tb}")
            r = cur.fetchone()
            if r and r[0] >= min_rows:
                return time.time() - start
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError(f"{db}.{res_tb}: expected >={min_rows} rows, timed out after {timeout}s")


class TestVtableStreamAggOptimizePerf:
    """Performance benchmark for the virtual-super-table aggregation optimization
    in stream calc (TS-7591 stream extension).

    The optimization is a compile-time planner decision (vstableAggOptimize +
    DYN_QTYPE_VTB_AGG) with no runtime toggle, so OFF-vs-ON cannot be measured
    in one build. The intended comparison is to run this same test against two
    builds:
      - the 3.0 branch  (optimization OFF — old row-materialization path)
      - this branch     (optimization ON  — DYN_QTYPE_VTB_AGG pushdown)
    and compare the logged elapsed time. There is therefore NO non-virtual
    baseline here — the baseline is the other build.

    Flow (stream calc only ingests data written AFTER the stream is created):
      1. create source schema (taosBenchmark, 0 rows)
      2. create virtual super table + child tables
      3. create snode + the stream (opens, waiting for data)
      4. load source data (taosBenchmark) AND co-timed trigger rows
      5. close the trigger window, time how long the stream takes to emit

    Catalog:
        - Streams:Performance
    Since: v3.4.0.0
    Labels: performance
    Jira: TS-7591
    History:
        - 2026-06-11 Created
        - 2026-06-12 Two-build OFF/ON design; dense co-timed trigger; no baseline
    """

    _DB = "vtable_stream_perf"
    _SRCS = [
        ("stb_int",    "stb_int",    "int",    "int_col",    "ctb_int"),
        ("stb_float",  "stb_float",  "float",  "float_col",  "ctb_float"),
        ("stb_double", "stb_double", "double", "double_col", "ctb_double"),
    ]

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # ------------------------------------------------------------------
    def _cleanup(self):
        for db in [self._DB] + [s[0] for s in self._SRCS]:
            try:
                tdSql.execute(f"drop database if exists {db} force")
            except Exception as e:
                tdLog.info(f"drop {db} skipped: {e}")

    def _load_source(self, rows, drop, child_exists):
        cfgdir = "/etc/taos"
        for dbname, stb, col_type, col_name, prefix in self._SRCS:
            _run_benchmark(_bench_json(cfgdir, dbname, stb, col_type, col_name,
                                       prefix, rows, drop, child_exists))
            tdLog.info(f"taosBenchmark ({rows} rows): {dbname}")

    def _create_vtables(self):
        tdSql.execute(f"create database if not exists {self._DB} vgroups 1 buffer 16 precision 'ms'")
        tdSql.execute(f"use {self._DB}")
        tdSql.execute(
            f"create stable if not exists {self._DB}.vst "
            f"(ts timestamp, c_int int, c_float float, c_double double) "
            f"tags (tint tinyint) virtual 1")
        for i in range(_CHILD_COUNT):
            tdSql.execute(
                f"create vtable if not exists {self._DB}.vct{i} ("
                f"  c_int    from stb_int.ctb_int{i}.int_col,"
                f"  c_float  from stb_float.ctb_float{i}.float_col,"
                f"  c_double from stb_double.ctb_double{i}.double_col"
                f") using {self._DB}.vst tags ({i % 10 + 1})")
        tdLog.info(f"created {_CHILD_COUNT} virtual child tables")

    def _trigger_rows(self, triggertb, span_ms):
        # Dense, co-timed with the data span: state_window(cint) with a run of
        # cint=1 across the data range, then a single cint=2 row to close the
        # window. Mirrors the proven functional-test trigger shape.
        rows = []
        step = max(span_ms // 12, 1000)  # ~12 trigger ticks across the span
        ts = _DATA_START_MS
        while ts < _DATA_START_MS + span_ms:
            rows.append(f"insert into {triggertb} values ({ts}, 1)")
            ts += step
        # closing row (state change) just past the data span
        rows.append(f"insert into {triggertb} values ({_DATA_START_MS + span_ms}, 2)")
        return rows

    # ------------------------------------------------------------------
    def test_vtable_stream_agg_optimize_perf(self):
        """Performance: virtual super table agg stream calc (single build).

        Run against the 3.0 build (optimization OFF) and this build (ON), then
        compare the logged elapsed time.

        Catalog:
            - Streams:Performance
        Since: v3.4.0.0
        Labels: performance
        Jira: TS-7591
        History:
            - 2026-06-11 Created
        """
        tdLog.info(f"=== scale: {_CHILD_COUNT} children x {_INSERT_ROWS} rows")

        tdLog.info("=== Step 0: cleanup")
        self._cleanup()

        tdLog.info("=== Step 1: source schema (0 rows)")
        self._load_source(rows=0, drop="yes", child_exists="no")

        tdLog.info("=== Step 2: virtual super table")
        self._create_vtables()

        tdLog.info("=== Step 3: snode + trigger table + stream")
        try:
            tdStream.createSnode()
        except Exception as e:
            tdLog.info(f"createSnode skipped: {e}")

        trig = f"{self._DB}.trig"
        tdSql.execute(f"create table if not exists {trig} (ts timestamp, cint int)")
        tdSql.execute(
            f"create stream s_vt_agg state_window(cint) from {trig} "
            f"into {self._DB}.res_vt_agg as "
            f"select _twstart, count(c_int), sum(c_int), avg(c_float), avg(c_double) "
            f"from {self._DB}.vst")

        conn = taos.connect(host="127.0.0.1", user="root", password="taosdata")

        tdLog.info("=== Step 4: load source data (stream ingests it)")
        self._load_source(rows=_INSERT_ROWS, drop="no", child_exists="yes")
        for dbname, stb, _, _, _ in self._SRCS:
            _wait_rows(conn, dbname, stb, _CHILD_COUNT * _INSERT_ROWS)

        # data span: rows * step, in ms
        span_ms = _INSERT_ROWS * _TIMESTAMP_STEP_MS

        tdLog.info("=== Step 5: insert co-timed trigger rows, close window, time it")
        t0 = time.time()
        tdSql.executes(self._trigger_rows(trig, span_ms))
        elapsed = _wait_stream_result(conn, self._DB, "res_vt_agg", 1)
        total = time.time() - t0

        # report
        cur = conn.cursor()
        cur.execute(f"select * from {self._DB}.res_vt_agg")
        rows = cur.fetchall()
        tdLog.info("=== Performance result ===")
        tdLog.info(f"stream emit elapsed: {elapsed:.2f}s (incl. trigger insert: {total:.2f}s)")
        tdLog.info(f"result rows: {len(rows)}; first row: {rows[0] if rows else None}")

        conn.close()
        tdLog.success(f"{__file__} passed (elapsed={elapsed:.2f}s)")
