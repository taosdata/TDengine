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
NOTIFY_PATH_SINGLE = "state_single_notify"
NOTIFY_PATH = "state_multi_notify"


class TestStreamNotifyState:
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
                    if event.streamName != stream_name and \
                       not event.streamName.endswith(f".{stream_name}"):
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

    def assert_json_null_payload(self, payload):
        if payload is not None:
            raise Exception(
                f"expected JSON null payload, got {payload}"
            )

    def find_matching_event(self, events, event_type, lhs_key, lhs_expected, rhs_key, rhs_expected):
        for event in events:
            if event.get("eventType") != event_type:
                continue
            if event.get(lhs_key) == lhs_expected and event.get(rhs_key) == rhs_expected:
                return event
        return None

    def find_event_by_type_and_field(self, events, event_type, field, expected):
        for event in events:
            if event.get("eventType") != event_type:
                continue
            if event.get(field) == expected:
                return event
        return None

    def verify_multi_state_events(self, events):
        if len(events) == 0:
            raise Exception("no state notify events received")

        for event in events:
            if event["eventType"] == "WINDOW_OPEN":
                if (
                    event.get("prevState") is not None
                    and not isinstance(event.get("prevState"), list)
                ):
                    raise Exception(f"invalid WINDOW_OPEN prevState: {event}")
                if not isinstance(event.get("curState"), list):
                    raise Exception(f"invalid WINDOW_OPEN payload: {event}")
                if (
                    event.get("prevState") is not None
                    and len(event["prevState"]) != 2
                ):
                    raise Exception(
                        f"unexpected WINDOW_OPEN prevState width: {event}"
                    )
                if len(event["curState"]) != 2:
                    raise Exception(f"unexpected WINDOW_OPEN state width: {event}")
            elif event["eventType"] == "WINDOW_CLOSE":
                if not isinstance(event.get("curState"), list) or not isinstance(event.get("nextState"), list):
                    raise Exception(f"invalid WINDOW_CLOSE payload: {event}")
                if len(event["curState"]) != 2 or len(event["nextState"]) != 2:
                    raise Exception(f"unexpected WINDOW_CLOSE state width: {event}")

        open_first = self.find_matching_event(
            events,
            "WINDOW_OPEN",
            "prevState",
            None,
            "curState",
            [1, True],
        )
        if open_first is None:
            raise Exception(
                "missing first WINDOW_OPEN event with JSON null prevState"
            )
        self.assert_json_null_payload(open_first["prevState"])
        self.assert_state_payload(open_first["curState"], [1, True])

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

    def verify_single_state_events(self, events):
        if len(events) == 0:
            raise Exception("no state notify events received")

        for event in events:
            if event["eventType"] == "WINDOW_OPEN":
                if (
                    event.get("prevState") is not None
                    and not isinstance(event.get("prevState"), list)
                ):
                    raise Exception(f"invalid WINDOW_OPEN prevState: {event}")
                if not isinstance(event.get("curState"), list):
                    raise Exception(f"invalid WINDOW_OPEN payload: {event}")
                if (
                    event.get("prevState") is not None
                    and len(event["prevState"]) != 1
                ):
                    raise Exception(
                        f"unexpected WINDOW_OPEN prevState width: {event}"
                    )
                if len(event["curState"]) != 1:
                    raise Exception(f"unexpected WINDOW_OPEN state width: {event}")
            elif event["eventType"] == "WINDOW_CLOSE":
                if not isinstance(event.get("curState"), list) or not isinstance(event.get("nextState"), list):
                    raise Exception(f"invalid WINDOW_CLOSE payload: {event}")
                if len(event["curState"]) != 1 or len(event["nextState"]) != 1:
                    raise Exception(f"unexpected WINDOW_CLOSE state width: {event}")

        open_first = self.find_matching_event(
            events,
            "WINDOW_OPEN",
            "prevState",
            None,
            "curState",
            [1],
        )
        if open_first is None:
            raise Exception(
                "missing first WINDOW_OPEN event with JSON null prevState"
            )
        self.assert_json_null_payload(open_first["prevState"])
        self.assert_state_payload(open_first["curState"], [1])

        close_1_2 = self.find_matching_event(
            events,
            "WINDOW_CLOSE",
            "curState",
            [1],
            "nextState",
            [2],
        )
        if close_1_2 is None:
            raise Exception("missing WINDOW_CLOSE event for [1] -> [2]")
        self.assert_state_payload(close_1_2["curState"], [1])
        self.assert_state_payload(close_1_2["nextState"], [2])

        open_1_2 = self.find_matching_event(
            events,
            "WINDOW_OPEN",
            "prevState",
            [1],
            "curState",
            [2],
        )
        if open_1_2 is None:
            raise Exception("missing WINDOW_OPEN event for [1] -> [2]")
        self.assert_state_payload(open_1_2["prevState"], [1])
        self.assert_state_payload(open_1_2["curState"], [2])

    def do_verify_state_notify_single(self, log_file):
        try:
            self.start_notify_server()
            time.sleep(1)
            self.ensure_snode()

            tdSql.execute("drop database if exists test_stream_notify_state_single;")
            tdSql.execute("create database test_stream_notify_state_single keep 3650;")
            tdSql.execute("use test_stream_notify_state_single;")
            tdSql.execute("create table ct0(ts timestamp, c1 int, v int);")
            tdSql.execute(
                f"""create stream s_notify_state_single state_window(c1) from ct0
                stream_options(event_type(WINDOW_CLOSE))
                notify('ws://localhost:{NOTIFY_PORT}/{NOTIFY_PATH_SINGLE}') on(window_open|window_close)
                into dst_notify_state_single as
                select _twstart, _twend, count(*) cnt, first(v) first_v
                from %%trows;"""
            )

            tdStream.checkStreamStatus("s_notify_state_single")

            tdSql.execute(
                """insert into ct0 values
                ('2025-01-01 00:00:00.000', 1, 10),
                ('2025-01-01 00:00:01.000', 1, 20),
                ('2025-01-01 00:00:02.000', 2, 30),
                ('2025-01-01 00:00:03.000', 3, 40),
                ('2025-01-01 00:00:04.000', 3, 50),
                ('2025-01-01 00:00:05.000', 1, 60);"""
            )

            self.wait_rows("dst_notify_state_single", 3)
            events = self.load_events(log_file, "s_notify_state_single")
            self.verify_single_state_events(events)
            print("state notify single .............. [ passed ]")
        finally:
            self.stop_notify_server()
            tdSql.execute("drop stream if exists s_notify_state_single;")
            tdSql.execute("drop database if exists test_stream_notify_state_single;")
            if os.path.exists(log_file):
                os.remove(log_file)

    def test_stream_notify_state_single(self):
        """Verify single-key state-window notify payloads

        1. Start the shared notify server and prepare an isolated test database
        2. Create a stream with single-key `state_window(c1)` notify settings
        3. Insert rows that trigger multiple state transitions
        4. Verify single-key payloads are unified as one-element arrays
        5. Assert representative transitions such as `[1] -> [2]`

        Catalog:
            - Stream

        Since: v3.4.1.0

        Labels: common,ci,stream,notify,state-window

        Jira: None

        History:
            - 2026-04-20 Tony Zhang add single-key list payload regression

        """
        log_file = os.path.join(
            NOTIFY_RESULT_DIR, f"{NOTIFY_PATH_SINGLE}.log")
        if os.path.exists(log_file):
            os.remove(log_file)

        self.do_verify_state_notify_single(log_file)

    def test_stream_notify_state_multi(self):
        """Verify multi-key state-window notify payloads

        1. Start the shared notify server and prepare an isolated test database
        2. Create a stream with multi-key `state_window(c1, c2)` notify settings
        3. Insert rows that trigger multiple state transitions across open and close events
          4. Verify the first `WINDOW_OPEN` uses JSON `null` for
              `prevState`, while subsequent multi-key state fields keep
              two-element arrays
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

    # ---------------------------------------------------------------
    # Test 3: Two-column state key with partial NULL in first row
    # ---------------------------------------------------------------
    def test_stream_notify_state_null_two_col(self):
        """Verify partial-NULL rows produce JSON null in state notify payloads (2 columns)

        Data pattern (c1 INT, c2 INT):
          Row 0: (10,  NULL)  <- c2 undefined, should appear as null in notify
          Row 1: (20,  300)   <- c1 differs (defined 10->20) -> new window
          Row 2: (NULL, 400)  <- c2 differs (defined 300->400) -> new window with null in c1
          Row 3: (30,  500)   <- c2 differs (defined 400->500) -> new window

        Note: partial-NULL rows where non-NULL columns MATCH the current
        state are "compatible" and absorbed into the current window
        (ri non-NULL, pi undefined -> not a change).  To trigger a window
        cut with a partial-NULL row, a DEFINED column must differ.

        Expected windows (3 closed + 1 open):
          W1: state=[10, null]                (Row 0 only)
          W2: state=[20, 300]                 (Row 1 only)
          W3: state=[null, 400]               (Row 2 only)
          W4: state=[30, 500]                 (Row 3, still open)

        Catalog:
            - Stream

        Since: v3.4.1.0

        Labels: common,ci,stream,notify,state-window,null-regression

        Jira: None

        History:
            - 2026-04-23 Tony Zhang created for P1 NULL semantics fix regression

        """
        notify_path = "state_null_2col"
        log_file = os.path.join(NOTIFY_RESULT_DIR, f"{notify_path}.log")
        db_name = "test_sn_null_2col"
        stream_name = "s_null_2col"
        dst_table = "dst_null_2col"

        if os.path.exists(log_file):
            os.remove(log_file)

        try:
            self.start_notify_server()
            time.sleep(1)
            self.ensure_snode()

            tdSql.execute(f"drop database if exists {db_name};")
            tdSql.execute(f"create database {db_name} keep 3650;")
            tdSql.execute(f"use {db_name};")
            tdSql.execute("create table ct0(ts timestamp, c1 int, c2 int, v int);")
            tdSql.execute(
                f"""create stream {stream_name} state_window(c1, c2) from ct0
                stream_options(event_type(WINDOW_CLOSE))
                notify('ws://localhost:{NOTIFY_PORT}/{notify_path}') on(window_open|window_close)
                into {dst_table} as
                select _twstart, _twend, count(*) cnt, first(v) first_v
                from %%trows;"""
            )
            tdStream.checkStreamStatus(stream_name)

            tdSql.execute(
                """insert into ct0 values
                ('2025-01-01 00:00:00.000', 10,   NULL, 1),
                ('2025-01-01 00:00:01.000', 20,   300,  2),
                ('2025-01-01 00:00:02.000', NULL,  400,  3),
                ('2025-01-01 00:00:03.000', 30,   500,  4);"""
            )

            self.wait_rows(dst_table, 3)
            events = self.load_events(log_file, stream_name)

            if len(events) == 0:
                raise Exception("no state notify events received")

            # --- W1 OPEN: prevState=null, curState=[10, null] ---
            first_open = self.find_matching_event(
                events, "WINDOW_OPEN", "prevState", None, "curState", [10, None]
            )
            if first_open is None:
                raise Exception(
                    f"missing first WINDOW_OPEN with prevState=null, curState=[10, null]. "
                    f"Got events: {events}"
                )

            # --- W1 CLOSE: curState=[10, null], nextState=[20, 300] ---
            close_w1 = self.find_matching_event(
                events, "WINDOW_CLOSE", "curState", [10, None], "nextState", [20, 300]
            )
            if close_w1 is None:
                raise Exception(
                    f"missing WINDOW_CLOSE for [10, null] -> [20, 300]. "
                    f"Got events: {events}"
                )

            # --- W2 CLOSE: curState=[20, 300], nextState=[null, 400] ---
            close_w2 = self.find_matching_event(
                events, "WINDOW_CLOSE", "curState", [20, 300], "nextState", [None, 400]
            )
            if close_w2 is None:
                raise Exception(
                    f"missing WINDOW_CLOSE for [20, 300] -> [null, 400]. "
                    f"Got events: {events}"
                )

            # --- W3 OPEN: prevState=[20, 300], curState=[null, 400] ---
            open_w3 = self.find_matching_event(
                events, "WINDOW_OPEN", "prevState", [20, 300], "curState", [None, 400]
            )
            if open_w3 is None:
                raise Exception(
                    f"missing WINDOW_OPEN for [20, 300] -> [null, 400]. "
                    f"Got events: {events}"
                )

            # --- W3 CLOSE: curState=[null, 400], nextState=[30, 500] ---
            close_w3 = self.find_matching_event(
                events, "WINDOW_CLOSE", "curState", [None, 400], "nextState", [30, 500]
            )
            if close_w3 is None:
                raise Exception(
                    f"missing WINDOW_CLOSE for [null, 400] -> [30, 500]. "
                    f"Got events: {events}"
                )

            tdLog.info("state notify null 2-col .............. [ passed ]")
        finally:
            self.stop_notify_server()
            tdSql.execute(f"drop stream if exists {stream_name};")
            tdSql.execute(f"drop database if exists {db_name};")
            if os.path.exists(log_file):
                os.remove(log_file)

    # ---------------------------------------------------------------
    # Test 4: Three-column state key with alternating partial NULLs
    # ---------------------------------------------------------------
    def test_stream_notify_state_null_three_col(self):
        """Verify three-column state key with partial-NULL patterns

        Data pattern (c1 INT, c2 INT, c3 INT):
          Row 0: (1,    NULL, NULL)  <- c2,c3 undefined
          Row 1: (2,    20,   NULL)  <- c1 differs (defined 1->2) -> new window
          Row 2: (3,    30,   300)   <- c1 differs (defined 2->3) -> new window
          Row 3: (4,    40,   400)   <- c1 differs (defined 3->4) -> new window

        Note: undefined columns (pi undefined) do NOT participate in
        state comparison, so only changes in DEFINED columns trigger cuts.

        Expected windows (3 closed + 1 open):
          W1: state=[1, null, null]           (Row 0 only)
          W2: state=[2, 20, null]             (Row 1 only)
          W3: state=[3, 30, 300]              (Row 2 only)
          W4: state=[4, 40, 400]              (Row 3, still open)

        Catalog:
            - Stream

        Since: v3.4.1.0

        Labels: common,ci,stream,notify,state-window,null-regression

        Jira: None

        History:
            - 2026-04-23 Tony Zhang created for P1 NULL semantics fix regression

        """
        notify_path = "state_null_3col"
        log_file = os.path.join(NOTIFY_RESULT_DIR, f"{notify_path}.log")
        db_name = "test_sn_null_3col"
        stream_name = "s_null_3col"
        dst_table = "dst_null_3col"

        if os.path.exists(log_file):
            os.remove(log_file)

        try:
            self.start_notify_server()
            time.sleep(1)
            self.ensure_snode()

            tdSql.execute(f"drop database if exists {db_name};")
            tdSql.execute(f"create database {db_name} keep 3650;")
            tdSql.execute(f"use {db_name};")
            tdSql.execute("create table ct0(ts timestamp, c1 int, c2 int, c3 int, v int);")
            tdSql.execute(
                f"""create stream {stream_name} state_window(c1, c2, c3) from ct0
                stream_options(event_type(WINDOW_CLOSE))
                notify('ws://localhost:{NOTIFY_PORT}/{notify_path}') on(window_open|window_close)
                into {dst_table} as
                select _twstart, _twend, count(*) cnt, first(v) first_v
                from %%trows;"""
            )
            tdStream.checkStreamStatus(stream_name)

            tdSql.execute(
                """insert into ct0 values
                ('2025-01-01 00:00:00.000', 1,    NULL, NULL, 1),
                ('2025-01-01 00:00:01.000', 2,    20,   NULL, 2),
                ('2025-01-01 00:00:02.000', 3,    30,   300,  3),
                ('2025-01-01 00:00:03.000', 4,    40,   400,  4);"""
            )

            self.wait_rows(dst_table, 3)
            events = self.load_events(log_file, stream_name)

            if len(events) == 0:
                raise Exception("no state notify events received")

            # --- W1 OPEN: curState=[1, null, null] ---
            first_open = self.find_matching_event(
                events, "WINDOW_OPEN", "prevState", None, "curState", [1, None, None]
            )
            if first_open is None:
                raise Exception(
                    f"missing first WINDOW_OPEN with curState=[1, null, null]. "
                    f"Got events: {events}"
                )

            # --- W1 CLOSE: [1, null, null] -> [2, 20, null] ---
            close_w1 = self.find_matching_event(
                events, "WINDOW_CLOSE", "curState", [1, None, None],
                "nextState", [2, 20, None]
            )
            if close_w1 is None:
                raise Exception(
                    f"missing WINDOW_CLOSE for [1, null, null] -> [2, 20, null]. "
                    f"Got events: {events}"
                )

            # --- W2 OPEN: prev=[1, null, null], cur=[2, 20, null] ---
            open_w2 = self.find_matching_event(
                events, "WINDOW_OPEN", "prevState", [1, None, None],
                "curState", [2, 20, None]
            )
            if open_w2 is None:
                raise Exception(
                    f"missing WINDOW_OPEN for [1, null, null] -> [2, 20, null]. "
                    f"Got events: {events}"
                )

            # --- W2 CLOSE: [2, 20, null] -> [3, 30, 300] ---
            close_w2 = self.find_matching_event(
                events, "WINDOW_CLOSE", "curState", [2, 20, None],
                "nextState", [3, 30, 300]
            )
            if close_w2 is None:
                raise Exception(
                    f"missing WINDOW_CLOSE for [2, 20, null] -> [3, 30, 300]. "
                    f"Got events: {events}"
                )

            # --- W3 CLOSE: [3, 30, 300] -> [4, 40, 400] ---
            close_w3 = self.find_matching_event(
                events, "WINDOW_CLOSE", "curState", [3, 30, 300],
                "nextState", [4, 40, 400]
            )
            if close_w3 is None:
                raise Exception(
                    f"missing WINDOW_CLOSE for [3, 30, 300] -> [4, 40, 400]. "
                    f"Got events: {events}"
                )

            tdLog.info("state notify null 3-col .............. [ passed ]")
        finally:
            self.stop_notify_server()
            tdSql.execute(f"drop stream if exists {stream_name};")
            tdSql.execute(f"drop database if exists {db_name};")
            if os.path.exists(log_file):
                os.remove(log_file)

    # ---------------------------------------------------------------
    # Test 5: All-NULL rows between transitions (deferred NULL)
    # ---------------------------------------------------------------
    def test_stream_notify_state_null_deferred(self):
        """Verify all-NULL rows between non-NULL rows do not corrupt state payloads

        Data pattern (c1 INT, c2 INT):
          Row 0: (10,  20)    <- both defined, first window
          Row 1: (NULL, NULL) <- all-NULL, deferred
          Row 2: (NULL, NULL) <- all-NULL, deferred
          Row 3: (30,  40)    <- both defined, state change -> new window

        Key assertion: curState in WINDOW_CLOSE must be [10, 20] (not corrupted
        by intermediate NULL rows), and nextState must be [30, 40].

        Catalog:
            - Stream

        Since: v3.4.1.0

        Labels: common,ci,stream,notify,state-window,null-regression

        Jira: None

        History:
            - 2026-04-23 Tony Zhang created for P1 NULL semantics fix regression

        """
        notify_path = "state_null_deferred"
        log_file = os.path.join(NOTIFY_RESULT_DIR, f"{notify_path}.log")
        db_name = "test_sn_null_defer"
        stream_name = "s_null_defer"
        dst_table = "dst_null_defer"

        if os.path.exists(log_file):
            os.remove(log_file)

        try:
            self.start_notify_server()
            time.sleep(1)
            self.ensure_snode()

            tdSql.execute(f"drop database if exists {db_name};")
            tdSql.execute(f"create database {db_name} keep 3650;")
            tdSql.execute(f"use {db_name};")
            tdSql.execute("create table ct0(ts timestamp, c1 int, c2 int, v int);")
            tdSql.execute(
                f"""create stream {stream_name} state_window(c1, c2) from ct0
                stream_options(event_type(WINDOW_CLOSE))
                notify('ws://localhost:{NOTIFY_PORT}/{notify_path}') on(window_open|window_close)
                into {dst_table} as
                select _twstart, _twend, count(*) cnt, first(v) first_v
                from %%trows;"""
            )
            tdStream.checkStreamStatus(stream_name)

            tdSql.execute(
                """insert into ct0 values
                ('2025-01-01 00:00:00.000', 10,   20,   1),
                ('2025-01-01 00:00:01.000', NULL,  NULL, 2),
                ('2025-01-01 00:00:02.000', NULL,  NULL, 3),
                ('2025-01-01 00:00:03.000', 30,   40,   4);"""
            )

            self.wait_rows(dst_table, 1)
            events = self.load_events(log_file, stream_name)

            if len(events) == 0:
                raise Exception("no state notify events received")

            # --- First WINDOW_OPEN: prevState=null, curState=[10, 20] ---
            first_open = self.find_matching_event(
                events, "WINDOW_OPEN", "prevState", None, "curState", [10, 20]
            )
            if first_open is None:
                raise Exception(
                    f"missing first WINDOW_OPEN with curState=[10, 20]. "
                    f"Got events: {events}"
                )

            # --- WINDOW_CLOSE: curState=[10, 20], nextState=[30, 40] ---
            # All-NULL rows must not corrupt the current window state
            close_10_20 = self.find_matching_event(
                events, "WINDOW_CLOSE", "curState", [10, 20], "nextState", [30, 40]
            )
            if close_10_20 is None:
                raise Exception(
                    f"missing WINDOW_CLOSE for [10, 20] -> [30, 40], "
                    f"all-NULL rows may have corrupted state. "
                    f"Got events: {events}"
                )

            # --- WINDOW_OPEN: prevState=[10, 20], curState=[30, 40] ---
            open_30_40 = self.find_matching_event(
                events, "WINDOW_OPEN", "prevState", [10, 20], "curState", [30, 40]
            )
            if open_30_40 is None:
                raise Exception(
                    f"missing WINDOW_OPEN for [10, 20] -> [30, 40]. "
                    f"Got events: {events}"
                )

            tdLog.info("state notify null deferred .............. [ passed ]")
        finally:
            self.stop_notify_server()
            tdSql.execute(f"drop stream if exists {stream_name};")
            tdSql.execute(f"drop database if exists {db_name};")
            if os.path.exists(log_file):
                os.remove(log_file)
