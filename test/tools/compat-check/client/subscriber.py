#!/usr/bin/env python3
# filepath: hot_update/client/subscriber.py
#
# Background TMQ subscriber process.
# Records time gaps between consecutive received message batches.
#
# Retry policy: on any consumer error reconnect and keep running (no fatal exit).
# Pass/fail criterion is checked in main.py:
#   if no data received for SUBSCRIBE_NO_DATA_TIMEOUT_S seconds → not ok.

import os
import sys
import time
import datetime

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

    consumer       = None
    last_msg_time  = 0.0   # local copy; also written to metrics for main.py check

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
                consumer = _make_consumer()
                # Signal writer that the subscription is active before it writes row 1
                if not metrics.get("subscribe_ready", False):
                    metrics["subscribe_ready"] = True
                    sys.stderr.write("   [subscriber] subscription active, writer may start\n")
            except Exception as e:
                metrics["subscribe_error_count"] = metrics.get("subscribe_error_count", 0) + 1
                sys.stderr.write(f"   [subscriber] reconnect error: {e}\n")
                time.sleep(config.RETRY_INTERVAL_S)
                continue

        # Poll for messages
        try:
            msg = consumer.poll(timeout=1.0)

            if msg is not None and msg.error() is None:
                # Count rows in this batch safely
                batch_rows = 0
                val = msg.value()
                if val:
                    running_total = metrics.get("subscribe_recv_total", 0)
                    for block in val:
                        data = block.fetchall()
                        if data:
                            for row in data:
                                running_total += 1
                                batch_rows    += 1
                                ts = row[0]
                                if isinstance(ts, datetime.datetime):
                                    ts_raw = int(ts.timestamp() * 1000)
                                elif isinstance(ts, (int, float)):
                                    ts_raw = int(ts)
                                else:
                                    ts_raw = ts   
                                #sys.stderr.write(f"   [subscriber] recv ts={ts_raw}  total={running_total}\n")

                now = time.time()
                if last_msg_time > 0:
                    gap = now - last_msg_time
                    metrics["subscribe_last_gap"] = gap
                    if gap > metrics.get("subscribe_max_gap", 0.0):
                        metrics["subscribe_max_gap"] = gap
                    if gap > metrics.get("subscribe_window_max", 0.0):
                        metrics["subscribe_window_max"] = gap
                last_msg_time = now
                # Expose last-received timestamp so main.py can apply 90s check
                metrics["subscribe_last_recv_time"] = now

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
            sys.stderr.write(f"   [subscriber] poll error: {e}\n")
            _close_consumer()
            time.sleep(config.RETRY_INTERVAL_S)

    _close_consumer()

