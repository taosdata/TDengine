#!/usr/bin/env python3

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
    retry_count = 0    # consecutive failure count
    retry_since = None # wall time when current retry window started

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

            # Success – reset consecutive failure counter
            retry_count = 0
            retry_since = None

            metrics["query_last_latency"] = latency
            if latency > metrics.get("query_max_latency", 0.0):
                metrics["query_max_latency"] = latency
            if latency > metrics.get("query_window_max", 0.0):
                metrics["query_window_max"] = latency
            metrics["query_phase4_success"] = metrics.get("query_phase4_success", 0) + 1
            if rows:
                metrics["query_last_count"] = int(rows[0][0])

        except Exception as e:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None

            retry_count += 1
            metrics["query_retry_count"] = metrics.get("query_retry_count", 0) + 1
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
                metrics["query_error_count"] = metrics.get("query_error_count", 0) + 1
                retry_count = 0
                retry_since = None
                sys.stderr.write(
                    f"[querier] failure threshold reached ({reason}), "
                    f"total failures={metrics['query_error_count']}, "
                    f"last error: {e}\n"
                )

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
