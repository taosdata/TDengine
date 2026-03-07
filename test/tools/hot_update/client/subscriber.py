#!/usr/bin/env python3
# filepath: hot_update/client/subscriber.py
#
# Background TMQ subscriber process.
# Records time gaps between consecutive received message batches.
#
# Retry policy: on any consumer error reconnect.
# If reconnection keeps failing for RETRY_TIMEOUT_S, marks
# metrics["subscribe_failed"] = True and exits.

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config


def subscriber_main(fqdn: str, cfg_dir: str, metrics, stop_event):
    """
    Parameters
    ----------
    fqdn       : cluster FQDN
    cfg_dir    : path to dnode1/cfg  (used for port info only; TMQ connects via IP)
    metrics    : multiprocessing.Manager().dict()
    stop_event : multiprocessing.Event
    """
    from taos.tmq import Consumer
    from taos.error import TmqError

    consumer    = None
    error_since = None
    last_msg_time: float = 0.0

    def _make_consumer():
        c = Consumer({
            "group.id":            "rolling_upgrade_verify",
            "td.connect.ip":       fqdn,
            "td.connect.port":     "6030",
            "auto.offset.reset":   "latest",
            "msg.with.table.name": "true",
            "enable.auto.commit":  "true",
        })
        c.subscribe([config.TOPIC_NAME])
        return c

    def _close_consumer():
        nonlocal consumer
        if consumer is not None:
            try:
                consumer.unsubscribe()
                consumer.close()
            except Exception:
                pass
            consumer = None

    while not stop_event.is_set():
        # Ensure consumer is alive
        if consumer is None:
            try:
                consumer    = _make_consumer()
                error_since = None
            except Exception as e:
                now = time.time()
                if error_since is None:
                    error_since = now
                metrics["subscribe_error_count"] = metrics.get("subscribe_error_count", 0) + 1

                if now - error_since >= config.RETRY_TIMEOUT_S:
                    metrics["subscribe_failed"] = True
                    sys.stderr.write(
                        f"[subscriber] FATAL: cannot connect for "
                        f"{config.RETRY_TIMEOUT_S}s, last error: {e}\n"
                    )
                    return
                time.sleep(config.RETRY_INTERVAL_S)
                continue

        # Poll for messages
        try:
            msg = consumer.poll(timeout=1.0)

            if msg is not None and msg.error() is None:
                # Count rows in this batch safely
                batch_rows = 0
                try:
                    for block in msg:
                        try:
                            batch_rows += len(block)
                        except Exception:
                            batch_rows += 1
                except Exception:
                    pass

                now = time.time()
                if last_msg_time > 0:
                    gap = now - last_msg_time
                    metrics["subscribe_last_gap"] = gap
                    if gap > metrics.get("subscribe_max_gap", 0.0):
                        metrics["subscribe_max_gap"] = gap
                    if gap > metrics.get("subscribe_window_max", 0.0):
                        metrics["subscribe_window_max"] = gap
                last_msg_time = now
                # Accumulate total rows received via TMQ
                if batch_rows > 0:
                    metrics["subscribe_last_batch_rows"] = batch_rows
                    metrics["subscribe_recv_total"] = (
                        metrics.get("subscribe_recv_total", 0) + batch_rows
                    )
                    metrics["subscribe_phase4_recv"] = (
                        metrics.get("subscribe_phase4_recv", 0) + batch_rows
                    )

        except Exception as e:
            metrics["subscribe_error_count"] = metrics.get("subscribe_error_count", 0) + 1
            now = time.time()
            if error_since is None:
                error_since = now
            if now - error_since >= config.RETRY_TIMEOUT_S:
                metrics["subscribe_failed"] = True
                sys.stderr.write(
                    f"[subscriber] FATAL: polling failed for "
                    f"{config.RETRY_TIMEOUT_S}s, last error: {e}\n"
                )
                _close_consumer()
                return
            _close_consumer()
            time.sleep(config.RETRY_INTERVAL_S)

    _close_consumer()
