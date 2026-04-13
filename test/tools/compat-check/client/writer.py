#!/usr/bin/env python3
# filepath: hot_update/client/writer.py
#
# Background writer process.
# Inserts 1 row per second into subtable d0.
#
# Retry policy
# ------------
# On failure: sleep RETRY_INTERVAL_S and retry (no process exit).
# After MAX_CONSECUTIVE_RETRIES consecutive failures increment write_error_count
# (= 1 real failure) and reset the counter.  The process keeps running.
# Pass/fail judgment is done in main.py: write_error_count == 0 → pass.

import os
import sys
import time
import random
import multiprocessing

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config


def writer_main(fqdn: str, cfg_dir: str, metrics, stop_event):
    """
    Parameters
    ----------
    fqdn       : cluster FQDN
    cfg_dir    : path to dnode1/cfg (passed to taos.connect)
    metrics    : multiprocessing.Manager().dict() – shared metrics
    stop_event : multiprocessing.Event – set by parent to request graceful shutdown
    """
    import taos

    conn         = None
    retry_count  = 0       # consecutive failure count
    retry_since  = None    # wall time when current retry window started
    ts           = 1789000000000

    # Wait until the subscriber has confirmed its subscription is active so that
    # no rows are written before TMQ offset is established (avoids leading gap).
    _wait_start = time.time()
    while not metrics.get("subscribe_ready", False):
        if time.time() - _wait_start > 30:
            sys.stderr.write("[writer] subscribe_ready timeout, starting anyway\n")
            break
        time.sleep(0.1)
    sys.stderr.write("   [writer] subscriber ready, starting writes\n")

    def _connect() -> taos.TaosConnection:
        return taos.connect(host=fqdn, config=cfg_dir)

    def _ensure_conn():
        nonlocal conn
        try:
            if conn is None:
                conn = _connect()
                conn.execute(f"USE {config.DB_NAME}")
        except Exception:
            conn = None
            raise

    while not stop_event.is_set():
        loop_start = time.time()
        try:
            _ensure_conn()
            ts     += 1000
            current = round(random.uniform(10.0, 15.0), 3)
            voltage = random.randint(200, 240)
            phase   = round(random.uniform(0.0, 1.0), 3)

            t0  = time.time()
            conn.execute(
                f"INSERT INTO d0 VALUES ({ts}, {current}, {voltage}, {phase})"
            )
            latency = time.time() - t0

            # Success – reset consecutive failure counter
            retry_count = 0
            retry_since = None

            metrics["write_last_latency"] = latency
            if latency > metrics.get("write_max_latency", 0.0):
                metrics["write_max_latency"] = latency
            if latency > metrics.get("write_window_max", 0.0):
                metrics["write_window_max"] = latency
            metrics["write_phase4_success"] = metrics.get("write_phase4_success", 0) + 1
            metrics["write_total_rows"]      = metrics.get("write_total_rows", 0) + 1
            metrics["write_last_ts"]      = ts
            metrics["write_last_current"] = current
            metrics["write_last_voltage"] = voltage
            metrics["write_last_phase"]   = phase

        except Exception as e:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None

            retry_count += 1
            metrics["write_retry_count"] = metrics.get("write_retry_count", 0) + 1
            now = time.time()
            if retry_since is None:
                retry_since = now
            elapsed_retry = now - retry_since
            hit_count = retry_count >= config.MAX_CONSECUTIVE_RETRIES
            hit_time  = elapsed_retry >= config.RETRY_MAX_DURATION_S
            if hit_count or hit_time:
                reason = (
                    f"retries={retry_count}/{config.MAX_CONSECUTIVE_RETRIES}  "
                    f"duration={elapsed_retry:.1f}s/{config.RETRY_MAX_DURATION_S}s"
                )
                metrics["write_error_count"] = metrics.get("write_error_count", 0) + 1
                retry_count = 0
                retry_since = None
                sys.stderr.write(
                    f"[writer] failure threshold reached ({reason}), "
                    f"total failures={metrics['write_error_count']}, "
                    f"last error: {e}\n"
                )

            time.sleep(config.RETRY_INTERVAL_S)
            continue

        # Sleep the remainder of the 1-second cycle
        elapsed = time.time() - loop_start
        sleep_time = max(0.0, 1.0 - elapsed)
        if sleep_time > 0:
            stop_event.wait(timeout=sleep_time)

    # Graceful shutdown
    if conn:
        try:
            conn.close()
        except Exception:
            pass
