#!/usr/bin/env python3
# filepath: hot_update/client/subscriber.py
#
# Background TMQ subscriber process.
# Records time gaps between consecutive received message batches.
#
# Retry policy: on any consumer error reconnect and keep running (no fatal exit).
# Pass/fail criterion is checked in main.py:
#   if no data received for SUBSCRIBE_NO_DATA_TIMEOUT_S seconds → not ok.

import collections
import os
import sys
import time
import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

# How many recently received (table, ts) keys the duplicate-delivery guard
# remembers.  Re-delivery after a reconnect replays from the head of the stream,
# so a window is enough, and it keeps the subscriber's memory flat however long
# the test runs.
_DEDUP_WINDOW = 200000


def _block_table(block) -> str:
    """Table name of a TMQ message block, or "" when unavailable.

    `MessageBlock.table()` exists in the bundled connector; older installations
    may not have it, so the caller has to cope with an empty name.
    """
    fn = getattr(block, "table", None)
    if callable(fn):
        try:
            return fn() or ""
        except Exception:
            return ""
    return ""


def _ts_in_ms(value) -> int:
    """Normalise the first column of a TMQ row (ts) to epoch milliseconds."""
    if isinstance(value, datetime.datetime):
        return int(value.timestamp() * 1000)
    if isinstance(value, (int, float)):
        return int(value)
    return value


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

    # Duplicate-delivery guard.  A reconnect (auto.offset.reset=latest plus auto
    # commit) can hand back rows that were already counted, which used to make
    # the "rows received == rows written" check fail by a row or two right after
    # a rolling upgrade.  Each (table, ts) is counted once, so duplicates no
    # longer inflate the total while real loss still shows up as a negative diff.
    #
    # It needs the table name: ts is only unique *within* one table, so keying
    # rows of different tables on ts alone would drop real rows and report them
    # as loss.  A connector whose message blocks carry no name therefore counts
    # every row, exactly as before the guard existed.
    seen_rows      = set()
    seen_order     = collections.deque()
    dedup_enabled  = True
    dup_rows       = 0

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
                        tbname = _block_table(block)
                        if not tbname and dedup_enabled:
                            # This connector does not name its blocks, so ts
                            # cannot identify a row on its own: count everything
                            # instead of dropping real rows as "duplicates".
                            dedup_enabled = False
                            # The guard stays off for the rest of the run, so the
                            # keys are dead weight now -- and the only thing here
                            # that keeps growing.  Drop them.
                            seen_rows.clear()
                            seen_order.clear()
                            sys.stderr.write("   [subscriber] message blocks carry no table "
                                             "name; duplicate-delivery guard disabled\n")
                        data = block.fetchall()
                        if data:
                            for row in data:
                                ts_raw = _ts_in_ms(row[0])
                                if dedup_enabled:
                                    key = (tbname, ts_raw)
                                    if key in seen_rows:
                                        dup_rows += 1
                                        continue
                                    seen_rows.add(key)
                                    seen_order.append(key)
                                    if len(seen_order) > _DEDUP_WINDOW:
                                        seen_rows.discard(seen_order.popleft())
                                running_total += 1
                                batch_rows    += 1
                                #sys.stderr.write(f"   [subscriber] recv ts={ts_raw}  total={running_total}\n")
                    metrics["subscribe_dup_total"] = dup_rows

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

