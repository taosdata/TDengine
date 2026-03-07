#!/usr/bin/env python3
# filepath: hot_update/client/querier.py
#
# Background querier process.
# Executes  SELECT COUNT(*) FROM {stable}  once per second.
# Updates shared metrics dict with latency information.
# Retry policy mirrors writer.py: RETRY_INTERVAL_S / RETRY_TIMEOUT_S.

import os
import sys
import time
import multiprocessing

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config


def querier_main(fqdn: str, cfg_dir: str, metrics, stop_event):
    """
    Parameters
    ----------
    fqdn       : cluster FQDN
    cfg_dir    : path to dnode1/cfg
    metrics    : multiprocessing.Manager().dict()
    stop_event : multiprocessing.Event
    """
    import taos

    conn        = None
    error_since = None

    def _connect() -> taos.TaosConnection:
        return taos.connect(host=fqdn, config=cfg_dir)

    def _ensure_conn():
        nonlocal conn
        try:
            if conn is None:
                conn = _connect()
        except Exception:
            conn = None
            raise

    sql = f"SELECT COUNT(*) FROM {config.DB_NAME}.{config.STABLE_NAME}"

    while not stop_event.is_set():
        loop_start = time.time()
        try:
            _ensure_conn()

            t0     = time.time()
            cursor = conn.cursor()
            cursor.execute(sql)
            rows   = cursor.fetchall()
            cursor.close()
            latency = time.time() - t0

            error_since = None

            metrics["query_last_latency"] = latency
            if latency > metrics.get("query_max_latency", 0.0):
                metrics["query_max_latency"] = latency
            if latency > metrics.get("query_window_max", 0.0):
                metrics["query_window_max"] = latency
            metrics["query_phase4_success"] = metrics.get("query_phase4_success", 0) + 1
            # Store COUNT(*) result for display
            if rows:
                metrics["query_last_count"] = int(rows[0][0])

        except Exception as e:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None

            now = time.time()
            if error_since is None:
                error_since = now
                metrics["query_error_count"] = metrics.get("query_error_count", 0) + 1

            if now - error_since >= config.RETRY_TIMEOUT_S:
                metrics["query_failed"] = True
                sys.stderr.write(
                    f"[querier] FATAL: query failed for {config.RETRY_TIMEOUT_S}s, "
                    f"last error: {e}\n"
                )
                return

            time.sleep(config.RETRY_INTERVAL_S)
            continue

        elapsed    = time.time() - loop_start
        sleep_time = max(0.0, 1.0 - elapsed)
        if sleep_time > 0:
            stop_event.wait(timeout=sleep_time)

    if conn:
        try:
            conn.close()
        except Exception:
            pass
