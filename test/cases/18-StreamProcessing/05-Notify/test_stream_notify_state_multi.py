###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, tdStream
from stream_notify_server import start_notify_server_background, stop_notify_server_background
from notify_check import NotifyLog
import os
import time

CALLER_FILE = os.path.realpath(__file__)
CALLER_DIR = os.path.dirname(CALLER_FILE)
NOTIFY_RESULT_DIR = os.path.join(CALLER_DIR, "notify_result_tmp")
NOTIFY_PORT = 12345
NOTIFY_PATH = "state_multi_notify"


class TestStreamNotifyStateMulti:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to execute {__file__}")
        tdSql.init(conn.cursor(), True)

    def start_notify_server(self):
        if os.path.isdir(NOTIFY_RESULT_DIR):
            for entry in os.listdir(NOTIFY_RESULT_DIR):
                path = os.path.join(NOTIFY_RESULT_DIR, entry)
                if os.path.isfile(path):
                    os.remove(path)
        else:
            os.makedirs(NOTIFY_RESULT_DIR, exist_ok=True)

        start_notify_server_background(port=NOTIFY_PORT, log_path=NOTIFY_RESULT_DIR)

    def stop_notify_server(self):
        stop_notify_server_background()

    def wait_rows(self, table_name, expected_rows, timeout_s=30):
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            tdSql.query(f"select count(*) from {table_name}")
            if tdSql.getData(0, 0) >= expected_rows:
                return
            time.sleep(1)
        tdLog.exit(f"table {table_name} did not reach {expected_rows} rows in time")

    def ensure_snode(self):
        tdSql.query("show snodes")
        if tdSql.getRows() == 0:
            tdStream.createSnode()

    def load_events(self, log_file, stream_name, timeout_s=10):
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            events = []
            try:
                for event in NotifyLog(log_file).events():
                    if event.streamName not in (stream_name, f"test_stream_notify_state_multi.{stream_name}"):
                        continue
                    if event.triggerType == "State":
                        events.append(event.raw)
            except FileNotFoundError:
                events = []

            if events:
                return events
            time.sleep(1)

        return []

    def assert_state_payload(self, payload, expected):
        if not isinstance(payload, list):
            raise Exception(f"expected list payload, got {payload}")
        if payload != expected:
            raise Exception(f"unexpected state payload, expected {expected}, got {payload}")

    def find_matching_event(self, events, event_type, lhs_key, lhs_expected, rhs_key, rhs_expected):
        for event in events:
            if event.get("eventType") != event_type:
                continue
            if event.get(lhs_key) == lhs_expected and event.get(rhs_key) == rhs_expected:
                return event
        return None

    def verify_multi_state_events(self, events):
        if len(events) == 0:
            raise Exception("no state notify events received")

        for event in events:
            if event["eventType"] == "WINDOW_OPEN":
                if not isinstance(event.get("prevState"), list) or not isinstance(event.get("curState"), list):
                    raise Exception(f"invalid WINDOW_OPEN payload: {event}")
                if len(event["prevState"]) != 2 or len(event["curState"]) != 2:
                    raise Exception(f"unexpected WINDOW_OPEN state width: {event}")
            elif event["eventType"] == "WINDOW_CLOSE":
                if not isinstance(event.get("curState"), list) or not isinstance(event.get("nextState"), list):
                    raise Exception(f"invalid WINDOW_CLOSE payload: {event}")
                if len(event["curState"]) != 2 or len(event["nextState"]) != 2:
                    raise Exception(f"unexpected WINDOW_CLOSE state width: {event}")

        close_1_true = self.find_matching_event(
            events,
            "WINDOW_CLOSE",
            "curState",
            [1, True],
            "nextState",
            [1, False],
        )
        if close_1_true is None:
            raise Exception("missing WINDOW_CLOSE event for [1, True] -> [1, False]")
        self.assert_state_payload(close_1_true["curState"], [1, True])
        self.assert_state_payload(close_1_true["nextState"], [1, False])

        open_1_false = self.find_matching_event(
            events,
            "WINDOW_OPEN",
            "prevState",
            [1, True],
            "curState",
            [1, False],
        )
        if open_1_false is None:
            raise Exception("missing WINDOW_OPEN event for [1, True] -> [1, False]")
        self.assert_state_payload(open_1_false["prevState"], [1, True])
        self.assert_state_payload(open_1_false["curState"], [1, False])

        close_2_false = self.find_matching_event(
            events,
            "WINDOW_CLOSE",
            "curState",
            [2, False],
            "nextState",
            [2, True],
        )
        if close_2_false is None:
            raise Exception("missing WINDOW_CLOSE event for [2, False] -> [2, True]")
        self.assert_state_payload(close_2_false["curState"], [2, False])
        self.assert_state_payload(close_2_false["nextState"], [2, True])

    def test_stream_notify_state_multi(self):
        """Verify multi-key state-window notify payloads

        1. Start the shared notify server and prepare an isolated test database
        2. Create a stream with multi-key `state_window(c1, c2)` notify settings
        3. Insert rows that trigger multiple state transitions across open and close events
        4. Verify notify payloads use two-element state arrays for `prevState`, `curState`, and `nextState`
        5. Assert representative transitions such as `[1, True] -> [1, False]` and `[2, False] -> [2, True]`

        Catalog:
            - Stream

        Since: v3.4.1.0

        Labels: common,ci,stream,notify,state-window

        History:
            - 2026-04-09 Tony Zhang created

        """
        log_file = os.path.join(NOTIFY_RESULT_DIR, f"{NOTIFY_PATH}.log")
        if os.path.exists(log_file):
            os.remove(log_file)

        try:
            self.start_notify_server()
            time.sleep(1)
            self.ensure_snode()

            tdSql.execute("drop database if exists test_stream_notify_state_multi;")
            tdSql.execute("create database test_stream_notify_state_multi keep 3650;")
            tdSql.execute("use test_stream_notify_state_multi;")
            tdSql.execute("create table ct0(ts timestamp, c1 int, c2 bool, v int);")
            tdSql.execute(
                f"""create stream s_notify_state_multi state_window(c1, c2) from ct0
                stream_options(event_type(WINDOW_CLOSE))
                notify('ws://localhost:{NOTIFY_PORT}/{NOTIFY_PATH}') on(window_open|window_close)
                into dst_notify_state_multi as
                select _twstart, _twend, count(*) cnt, first(v) first_v
                from %%trows;"""
            )

            tdStream.checkStreamStatus("s_notify_state_multi")

            tdSql.execute(
                """insert into ct0 values
                ('2025-01-01 00:00:00.000', 1, true, 10),
                ('2025-01-01 00:00:01.000', 1, true, 20),
                ('2025-01-01 00:00:02.000', 1, false, 30),
                ('2025-01-01 00:00:03.000', 2, false, 40),
                ('2025-01-01 00:00:04.000', 2, true, 50),
                ('2025-01-01 00:00:05.000', 1, true, 60);"""
            )

            self.wait_rows("dst_notify_state_multi", 4)
            events = self.load_events(log_file, "s_notify_state_multi")
            self.verify_multi_state_events(events)
        finally:
            self.stop_notify_server()
            tdSql.execute("drop stream if exists s_notify_state_multi;")
            tdSql.execute("drop database if exists test_stream_notify_state_multi;")
            if os.path.exists(log_file):
                os.remove(log_file)