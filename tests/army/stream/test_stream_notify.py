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

from frame import etool
from frame.etool import *
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame.common import *
from streamNotifyServer import StreamNotifyServer

class TestStreamNotifySinglePass():
    def init(self, num_addr_per_stream, trigger_mode, notify_event):
        self.num_addr_per_stream = num_addr_per_stream
        self.trigger_mode = trigger_mode
        self.notify_event = notify_event
        self.streams = []

    def __destroy__(self):
        pass

    def is_port_in_use(self, port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(("127.0.0.1", port)) == 0

    def gen_streams(self):
        self.streams = [
            {
                "stream_name": "s_time_par",
                "dest_table": "dst_time_par",
                "window_clause": "interval(5s)",
                "partitioned": True,
            },
            {
                "stream_name": "s_time",
                "dest_table": "dst_time",
                "window_clause": "interval(5s)",
                "partitioned": False,
            },
            {
                "stream_name": "s_state_par",
                "dest_table": "dst_state_par",
                "window_clause": "state_window(c6)",
                "partitioned": True,
            },
            {
                "stream_name": "s_session_par",
                "dest_table": "dst_session_par",
                "window_clause": "session(50a)",
                "partitioned": True,
            },
            {
                "stream_name": "s_session",
                "dest_table": "dst_session",
                "window_clause": "session(50a)",
                "partitioned": False,
            },
            {
                "stream_name": "s_event_par",
                "dest_table": "dst_event_par",
                "window_clause": "event_window start with c6 = true end with c6 = false",
                "partitioned": True,
            },
            {
                "stream_name": "s_count_par",
                "dest_table": "dst_count_par",
                "window_clause": "count_window(10)",
                "partitioned": True,
            },
        ]
        port = 10000
        for stream in self.streams:
            stream["notify_address"] = ""
            stream["notify_server"] = []
            for i in range(self.num_addr_per_stream):
                # Find an available port
                while self.is_port_in_use(port):
                    port += 1
                # Start the stream notify server and add the address to the stream
                server = StreamNotifyServer()
                server.run(port, f"{stream['stream_name']}_{i}.log")
                stream["notify_address"] += f"ws://127.0.0.1:{port},"
                stream["notify_server"].append(server)
                port += 1
            stream["notify_address"] = stream["notify_address"][:-1]

    def create_streams(self):
        tdLog.info("==========step1:create table")
        tdSql.execute("drop database if exists test;")
        tdSql.execute("create database test keep 3650;")
        tdSql.execute("use test;")
        tdSql.execute(
            f"""create table if not exists test.st
            (ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 double, c5 float, c6 bool, c7 varchar(10), c8 nchar(10), c9 tinyint unsigned, c10 smallint unsigned, c11 int unsigned, c12 bigint unsigned);
            """
        )
        for stream in self.streams:
            stream_sql = f"""create stream {stream["stream_name"]} TRIGGER {self.trigger_mode} IGNORE EXPIRED 0 IGNORE UPDATE 0 into {stream["dest_table"]} as
                        select _wstart, _wend, min(c0), max(c1), min(c2), max(c3), min(c4), max(c5), first(c6), first(c7), last(c8), min(c9), max(c10), min(c11), max(c12)
                        from test.st {stream["partitioned"] and "partition by tbname" or ""}
                        {stream["window_clause"]}
                        notify ({stream["notify_address"]}) on ({self.notify_event});
                        """
            tdSql.execute(stream_sql, show=True)
        # Wait for the stream tasks to be ready
        for i in range(50):
            time.sleep(1)
            rows = tdSql.query("select * from information_schema.ins_stream_tasks where status <> 'ready';")
            if rows == 0:
                break
            tdLog.info(f"i={i} wait for stream tasks ready ...")

    def insert_data(self):
        tdLog.info("insert stream notify test data.")
        # taosBenchmark run
        json = etool.curFile(__file__, "stream_notify.json")
        etool.benchMark(json=json)

    def wait_all_streams_done(self):
        for i in range(100):
            time.sleep(1)
            rows = tdSql.query("select info from information_schema.ins_stream_tasks where level <> 'sink';")
            allDone = True
            for i in range(rows):
                info = tdSql.getData(i, 0)
                if self.trigger_mode == "FORCE_WINDOW_CLOSE":
                    if not info.start_with("2025-02"):
                        allDone = False
                        break
                else:
                    if info.split(" ")[0] != info.split(" ")[2][:-1]:
                        allDone = False
                        break
            if allDone:
                break
            tdLog.info("wait for all streams done ...")
        # Stop all the stream notify servers
        for stream in self.streams:
            for server in stream["notify_server"]:
                server.stop()

    def parse(self, log_file, out_file, stream_name):
        message_ids = set()
        events_map = {}
        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                data = json.loads(line)

                # Check if the data has the required fields: messageId, timestamp, streams
                if "messageId" not in data:
                    print(f"Error: Missing 'messageId' in data {data}")
                    return False
                if "timestamp" not in data:
                    print(f"Error: Missing 'timestamp' in data {data}")
                    return False
                if "streams" not in data:
                    print(f"Error: Missing 'streams' in data {data}")
                    return False

                # Check if the message id is duplicated
                if message_ids.__contains__(data["messageId"]):
                    print(f"Error: Duplicate message id {data['messageId']}")
                    return False
                message_ids.add(data["messageId"])

                # Check if the streams is correct
                for stream in data["streams"]:
                    # Check if the stream has the required fields: streamName, events
                    if "streamName" not in stream:
                        print(f"Error: Missing 'streamName' in stream {stream}")
                        return False
                    if "events" not in stream:
                        print(f"Error: Missing 'events' in stream {stream}")
                        return False

                    # Check if the stream name is correct
                    if stream["streamName"] != stream_name:
                        print(f"Error: Incorrect stream name {stream['streamName']}")
                        return False

                    # Check if the events are correct
                    for event in stream["events"]:
                        # Check if the event has the required fields: tableName, eventType, eventTime, windowId, windowType
                        if "tableName" not in event:
                            print(f"Error: Missing 'tableName' in event {event}")
                            return False
                        if "eventType" not in event:
                            print(f"Error: Missing 'eventType' in event {event}")
                            return False
                        if "eventTime" not in event:
                            print(f"Error: Missing 'eventTime' in event {event}")
                            return False
                        if "windowId" not in event:
                            print(f"Error: Missing 'windowId' in event {event}")
                            return False
                        if "windowType" not in event:
                            print(f"Error: Missing 'windowType' in event {event}")
                            return False
                        if event["eventType"] not in [
                            "WINDOW_OPEN",
                            "WINDOW_CLOSE",
                            "WINDOW_INVALIDATION",
                        ]:
                            print(f"Error: Invalid event type {event['eventType']}")
                            return False
                        if event["windowType"] not in [
                            "Time",
                            "State",
                            "Session",
                            "Event",
                            "Count",
                        ]:
                            print(f"Error: Invalid window type {event['windowType']}")
                            return False

                        if event["eventType"] == "WINDOW_INVALIDATION":
                            # WINDOW_INVALIDATION must have fields: windowStart, windowEnd
                            if "windowStart" not in event:
                                print(f"Error: Missing 'windowStart' in event {event}")
                                return False
                            if "windowEnd" not in event:
                                print(f"Error: Missing 'windowEnd' in event {event}")
                                return False
                            events_map.pop(
                                (event["tableName"], event["windowId"]), None
                            )
                            continue

                        # Get the event from the event map; if it doesn't exist, create a new one
                        e = events_map.get((event["tableName"], event["windowId"]))
                        if e is None:
                            events_map[(event["tableName"], event["windowId"])] = {
                                "opened": False,
                                "closed": False,
                                "wstart": 0,
                                "wend": 0,
                            }
                            e = events_map.get((event["tableName"], event["windowId"]))

                        if event["eventType"] == "WINDOW_OPEN":
                            # WINDOW_OPEN for all windows must have field: windowStart
                            if "windowStart" not in event:
                                print(f"Error: Missing 'windowStart' in event {event}")
                                return False
                            if event["windowType"] == "State":
                                # WINDOW_OPEN for State window must also have fields: prevState, curState
                                if "prevState" not in event:
                                    print(
                                        f"Error: Missing 'prevState' in event {event}"
                                    )
                                    return False
                                if "curState" not in event:
                                    print(f"Error: Missing 'curState' in event {event}")
                                    return False
                            elif event["windowType"] == "Event":
                                # WINDOW_OPEN for Event window must also have fields: triggerCondition
                                if "triggerCondition" not in event:
                                    print(
                                        f"Error: Missing 'triggerCondition' in event {event}"
                                    )
                                    return False
                            e["opened"] = True
                            e["wstart"] = event["windowStart"]
                        elif event["eventType"] == "WINDOW_CLOSE":
                            # WINDOW_CLOSE for all windows must have fields: windowStart, windowEnd, result
                            if "windowStart" not in event:
                                print(f"Error: Missing 'windowStart' in event {event}")
                                return False
                            if "windowEnd" not in event:
                                print(f"Error: Missing 'windowEnd' in event {event}")
                                return False
                            if "result" not in event:
                                print(f"Error: Missing 'result' in event {event}")
                                return False
                            if event["windowType"] == "State":
                                # WINDOW_CLOSE for State window must also have fields: curState, nextState
                                if "curState" not in event:
                                    print(f"Error: Missing 'curState' in event {event}")
                                    return False
                                if "nextState" not in event:
                                    print(
                                        f"Error: Missing 'nextState' in event {event}"
                                    )
                                    return False
                            elif event["windowType"] == "Event":
                                # WINDOW_CLOSE for Event window must also have fields: triggerCondition
                                if "triggerCondition" not in event:
                                    print(
                                        f"Error: Missing 'triggerCondition' in event {event}"
                                    )
                                    return False
                            e["closed"] = True
                            e["wstart"] = event["windowStart"]
                            e["wend"] = event["windowEnd"]

        # Collect all the windows that closed
        windows_map = {}
        for k, v in events_map.items():
            if not v["closed"]:
                continue
            e = windows_map.get(k[0])
            if e is None:
                windows_map[k[0]] = []
                e = windows_map.get(k[0])
            e.append((v["wstart"], v["wend"]))

        # Sort the windows by start time
        for k, v in windows_map.items():
            v.sort(key=lambda x: x[0])

        # Write all collected window info to the specified output file in sorted order as csv format
        with open(out_file, "w", encoding="utf-8") as f:
            f.write("wstart,wend,tbname\n")
            for k, v in sorted(windows_map.items()):
                for w in v:
                    f.write(f"{w[0]},{w[1]},{k}\n")
        return True

    def check_notify_result(self):
        all_right = True
        for stream in self.streams:
            if stream["notify_server"].length == 0:
                continue
            query_file = f"query_{stream['stream_name']}.csv"
            tdSql.execute(f"select cast(_wstart as bigint) as wstart, cast(_wend as bigint) as wend, tbname from {stream['dest_table']} order by tbname, wstart >> {query_file};")
            for i in range(self.num_addr_per_stream):
                server = stream["notify_server"][i]
                parse_file = f"{stream['stream_name']}_{i}.csv"
                self.parse(f"{stream['stream_name']}_{i}.log", parse_file, stream["stream_name"])
                # Compare the result using diff command
                diff_file = f"diff_{stream['stream_name']}_{i}.csv"
                os.system(f"diff {query_file} {parse_file} > {diff_file}")
                if os.path.getsize(diff_file) != 0:
                    tdLog.error(f"Error: {stream['stream_name']}_{i} notify result is not correct")
                    all_right = False
        if not all_right:
            raise Exception("Error: notify result is not correct")

    def drop_all_streams(self):
        for stream in self.streams:
            tdSql.execute(f"drop stream if exists {stream['stream_name']};")

    def run(self):
        self.gen_streams()
        self.create_streams()
        self.insert_data()
        self.wait_all_streams_done()
        self.check_notify_result()
        self.drop_all_streams()
        tdLog.info(f"TestStreamNotifySinglePass({self.num_addr_per_stream}, {self.trigger_mode}, {self.notify_event}) successfully executed")

class TDTestCase(TBase):
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def run(self):
        p = TestStreamNotifySinglePass(1, "WINDOW_CLOSE", "'window_open', 'window_close'")
        p.run()

    def stop(self):
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
