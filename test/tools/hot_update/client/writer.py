#!/usr/bin/env python3
# filepath: hot_update/client/writer.py
#
# Background writer process.
# Inserts 1 row per second into subtable d0.
# Updates shared metrics dict with latency information.
# On failure: retries every RETRY_INTERVAL_S seconds.
# If retries exceed RETRY_TIMEOUT_S, marks metrics["write_failed"] = True and exits.

import os
import sys
import time
import random
import multiprocessing

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config


def writer_main(fqdn: str, cfg_dir: str, metrics, stop_event):
    """
    Entry point for the writer child process.

    Parameters
    ----------
    fqdn       : cluster FQDN
    cfg_dir    : path to dnode1/cfg (passed to taos.connect)
    metrics    : multiprocessing.Manager().dict() – shared metrics
    stop_event : multiprocessing.Event – set by parent to request graceful shutdown
    """
    # Import taos inside the worker; it inherits LD_LIBRARY_PATH from the parent
    # (Linux fork), so the correct libtaos.so is already loaded.
    import taos

    conn         = None
    error_since  = None   # timestamp of first consecutive failure in a retry window
    ts           = 1789000000000

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

            # Reset error window on success
            error_since = None

            # Update metrics
            metrics["write_last_latency"] = latency
            if latency > metrics.get("write_max_latency", 0.0):
                metrics["write_max_latency"] = latency
            if latency > metrics.get("write_window_max", 0.0):
                metrics["write_window_max"] = latency
            metrics["write_phase4_success"] = metrics.get("write_phase4_success", 0) + 1
            # Store last-written row for display
            metrics["write_last_ts"]      = ts
            metrics["write_last_current"] = current
            metrics["write_last_voltage"] = voltage
            metrics["write_last_phase"]   = phase

        except Exception as e:
            # Reconnect next iteration
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None

            now = time.time()
            if error_since is None:
                error_since = now
                metrics["write_error_count"] = metrics.get("write_error_count", 0) + 1

            if now - error_since >= config.RETRY_TIMEOUT_S:
                metrics["write_failed"] = True
                sys.stderr.write(
                    f"[writer] FATAL: write failed for {config.RETRY_TIMEOUT_S}s, "
                    f"last error: {e}\n"
                )
                return

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
